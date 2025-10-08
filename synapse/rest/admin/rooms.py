#
# This file is licensed under the Affero General Public License (AGPL) version 3.
#
# Copyright 2019-2021 The Matrix.org Foundation C.I.C.
# Copyright (C) 2023 New Vector, Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# See the GNU Affero General Public License for more details:
# <https://www.gnu.org/licenses/agpl-3.0.html>.
#
# Originally licensed under the Apache License, Version 2.0:
# <http://www.apache.org/licenses/LICENSE-2.0>.
#
# [This file includes modifications made by New Vector Limited]
#
#
import logging
from http import HTTPStatus
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, cast

import attr
from immutabledict import immutabledict

from synapse.api.constants import Direction, EventTypes, JoinRules, Membership
from synapse.api.room_versions import EventFormatVersions, RoomVersions
from synapse.api.errors import AuthError, Codes, NotFoundError, SynapseError
from synapse.events import EventBase, make_event_from_dict
from synapse.events.snapshot import EventContext
from synapse.api.filtering import Filter
from synapse.handlers.pagination import (
    PURGE_ROOM_ACTION_NAME,
    SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME,
)
from synapse.http.servlet import (
    ResolveRoomIdMixin,
    RestServlet,
    assert_params_in_dict,
    parse_boolean,
    parse_enum,
    parse_integer,
    parse_json,
    parse_json_object_from_request,
    parse_string,
)
from synapse.http.site import SynapseRequest
from synapse.rest.admin._base import (
    admin_patterns,
    assert_requester_is_admin,
    assert_user_is_admin,
)
from synapse.storage.databases.main.room import RoomSortOrder
from synapse.streams.config import PaginationConfig
from synapse.types import JsonDict, RoomID, ScheduledTask, UserID, create_requester
from synapse.types.state import StateFilter

if TYPE_CHECKING:
    from synapse.api.auth import Auth
    from synapse.events import EventBase
    from synapse.handlers.pagination import PaginationHandler
    from synapse.handlers.room import RoomShutdownHandler
    from synapse.server import HomeServer

logger = logging.getLogger(__name__)


class RoomRestV2Servlet(RestServlet):
    """Delete a room from server asynchronously with a background task.

    It is a combination and improvement of shutdown and purge room.

    Shuts down a room by removing all local users from the room.
    Blocking all future invites and joins to the room is optional.

    If desired any local aliases will be repointed to a new room
    created by `new_room_user_id` and kicked users will be auto-
    joined to the new room.

    If 'purge' is true, it will remove all traces of a room from the database.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)$", "v2")

    def __init__(self, hs: "HomeServer"):
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main
        self._pagination_handler = hs.get_pagination_handler()
        self._third_party_rules = hs.get_module_api_callbacks().third_party_event_rules

    async def on_DELETE(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        requester = await self._auth.get_user_by_req(request)
        await assert_user_is_admin(self._auth, requester)

        content = parse_json_object_from_request(request)

        block = content.get("block", False)
        if not isinstance(block, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'block' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        purge = content.get("purge", True)
        if not isinstance(purge, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'purge' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        force_purge = content.get("force_purge", False)
        if not isinstance(force_purge, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'force_purge' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        if not RoomID.is_valid(room_id):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "%s is not a legal room ID" % (room_id,)
            )

        # Check this here, as otherwise we'll only fail after the background job has been started.
        if not await self._third_party_rules.check_can_shutdown_room(
            requester.user.to_string(), room_id
        ):
            raise SynapseError(
                403, "Shutdown of this room is forbidden", Codes.FORBIDDEN
            )

        delete_id = await self._pagination_handler.start_shutdown_and_purge_room(
            room_id=room_id,
            shutdown_params={
                "new_room_user_id": content.get("new_room_user_id"),
                "new_room_name": content.get("room_name"),
                "message": content.get("message"),
                "requester_user_id": requester.user.to_string(),
                "block": block,
                "purge": purge,
                "force_purge": force_purge,
            },
        )

        return HTTPStatus.OK, {"delete_id": delete_id}


def _convert_delete_task_to_response(task: ScheduledTask) -> JsonDict:
    return {
        "delete_id": task.id,
        "room_id": task.resource_id,
        "status": task.status,
        "shutdown_room": task.result,
    }


class DeleteRoomStatusByRoomIdRestServlet(RestServlet):
    """Get the status of the delete room background task."""

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/delete_status$", "v2")

    def __init__(self, hs: "HomeServer"):
        self._auth = hs.get_auth()
        self._pagination_handler = hs.get_pagination_handler()

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self._auth, request)

        if not RoomID.is_valid(room_id):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "%s is not a legal room ID" % (room_id,)
            )

        delete_tasks = await self._pagination_handler.get_delete_tasks_by_room(room_id)

        if delete_tasks:
            return HTTPStatus.OK, {
                "results": [
                    _convert_delete_task_to_response(task) for task in delete_tasks
                ],
            }
        else:
            raise NotFoundError("No delete task for room_id '%s' found" % room_id)


class DeleteRoomStatusByDeleteIdRestServlet(RestServlet):
    """Get the status of the delete room background task."""

    PATTERNS = admin_patterns("/rooms/delete_status/(?P<delete_id>[^/]*)$", "v2")

    def __init__(self, hs: "HomeServer"):
        self._auth = hs.get_auth()
        self._pagination_handler = hs.get_pagination_handler()

    async def on_GET(
        self, request: SynapseRequest, delete_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self._auth, request)

        delete_task = await self._pagination_handler.get_delete_task(delete_id)
        if delete_task is None or (
            delete_task.action != PURGE_ROOM_ACTION_NAME
            and delete_task.action != SHUTDOWN_AND_PURGE_ROOM_ACTION_NAME
        ):
            raise NotFoundError("delete id '%s' not found" % delete_id)

        return HTTPStatus.OK, _convert_delete_task_to_response(delete_task)


class ListRoomRestServlet(RestServlet):
    """
    List all rooms that are known to the homeserver. Results are returned
    in a dictionary containing room information. Supports pagination.
    """

    PATTERNS = admin_patterns("/rooms$")

    def __init__(self, hs: "HomeServer"):
        self.store = hs.get_datastores().main
        self.auth = hs.get_auth()
        self.admin_handler = hs.get_admin_handler()

    async def on_GET(self, request: SynapseRequest) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        # Extract query parameters
        start = parse_integer(request, "from", default=0)
        limit = parse_integer(request, "limit", default=100)
        order_by = parse_string(
            request,
            "order_by",
            default=RoomSortOrder.NAME.value,
            allowed_values=[sort_order.value for sort_order in RoomSortOrder],
        )

        search_term = parse_string(request, "search_term", encoding="utf-8")
        if search_term == "":
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "search_term cannot be an empty string",
                errcode=Codes.INVALID_PARAM,
            )

        public_rooms = parse_boolean(request, "public_rooms")
        empty_rooms = parse_boolean(request, "empty_rooms")

        direction = parse_enum(request, "dir", Direction, default=Direction.FORWARDS)
        reverse_order = True if direction == Direction.BACKWARDS else False

        # Return list of rooms according to parameters
        rooms, total_rooms = await self.store.get_rooms_paginate(
            start,
            limit,
            order_by,
            reverse_order,
            search_term,
            public_rooms,
            empty_rooms,
        )

        response = {
            # next_token should be opaque, so return a value the client can parse
            "offset": start,
            "rooms": rooms,
            "total_rooms": total_rooms,
        }

        # Are there more rooms to paginate through after this?
        if (start + limit) < total_rooms:
            # There are. Calculate where the query should start from next time
            # to get the next part of the list
            response["next_batch"] = start + limit

        # Is it possible to paginate backwards? Check if we currently have an
        # offset
        if start > 0:
            if start > limit:
                # Going back one iteration won't take us to the start.
                # Calculate new offset
                response["prev_batch"] = start - limit
            else:
                response["prev_batch"] = 0

        return HTTPStatus.OK, response


class RoomRestServlet(RestServlet):
    """Manage a room.

    On GET : Get details of a room.

    On DELETE : Delete a room from server.

    It is a combination and improvement of shutdown and purge room.

    Shuts down a room by removing all local users from the room.
    Blocking all future invites and joins to the room is optional.

    If desired any local aliases will be repointed to a new room
    created by `new_room_user_id` and kicked users will be auto-
    joined to the new room.

    If 'purge' is true, it will remove all traces of a room from the database.

    TODO: Add on_POST to allow room creation without joining the room
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)$")

    def __init__(self, hs: "HomeServer"):
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main
        self.room_shutdown_handler = hs.get_room_shutdown_handler()
        self.pagination_handler = hs.get_pagination_handler()

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        ret = await self.store.get_room_with_stats(room_id)
        if not ret:
            raise NotFoundError("Room not found")

        members = await self.store.get_users_in_room(room_id)
        result = attr.asdict(ret)
        result["joined_local_devices"] = await self.store.count_devices_by_users(
            members
        )
        result["forgotten"] = await self.store.is_locally_forgotten_room(room_id)

        return HTTPStatus.OK, result

    async def on_DELETE(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        return await self._delete_room(
            request,
            room_id,
            self.auth,
            self.room_shutdown_handler,
            self.pagination_handler,
        )

    async def _delete_room(
        self,
        request: SynapseRequest,
        room_id: str,
        auth: "Auth",
        room_shutdown_handler: "RoomShutdownHandler",
        pagination_handler: "PaginationHandler",
    ) -> Tuple[int, JsonDict]:
        requester = await auth.get_user_by_req(request)
        await assert_user_is_admin(auth, requester)

        content = parse_json_object_from_request(request)

        block = content.get("block", False)
        if not isinstance(block, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'block' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        purge = content.get("purge", True)
        if not isinstance(purge, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'purge' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        force_purge = content.get("force_purge", False)
        if not isinstance(force_purge, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'force_purge' must be a boolean, if given",
                Codes.BAD_JSON,
            )

        ret = await room_shutdown_handler.shutdown_room(
            room_id=room_id,
            params={
                "new_room_user_id": content.get("new_room_user_id"),
                "new_room_name": content.get("room_name"),
                "message": content.get("message"),
                "requester_user_id": requester.user.to_string(),
                "block": block,
                "purge": purge,
                "force_purge": force_purge,
            },
        )

        # Purge room
        if purge:
            try:
                await pagination_handler.purge_room(room_id, force=force_purge)
            except NotFoundError:
                if block:
                    # We can block unknown rooms with this endpoint, in which case
                    # a failed purge is expected.
                    pass
                else:
                    # But otherwise, we expect this purge to have succeeded.
                    raise

        # Cast safety: cast away the knowledge that this is a TypedDict.
        # See https://github.com/python/mypy/issues/4976#issuecomment-579883622
        # for some discussion on why this is necessary. Either way,
        # `ret` is an opaque dictionary blob as far as the rest of the app cares.
        return HTTPStatus.OK, cast(JsonDict, ret)


class RoomMembersRestServlet(RestServlet):
    """
    Get members list of a room.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/members$")

    def __init__(self, hs: "HomeServer"):
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        room = await self.store.get_room(room_id)
        if not room:
            raise NotFoundError("Room not found")

        members = await self.store.get_users_in_room(room_id)
        ret = {"members": members, "total": len(members)}

        return HTTPStatus.OK, ret


class RoomStateRestServlet(RestServlet):
    """
    Get full state within a room.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/state$")

    def __init__(self, hs: "HomeServer"):
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main
        self._storage_controllers = hs.get_storage_controllers()
        self.clock = hs.get_clock()
        self._event_serializer = hs.get_event_client_serializer()

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        room = await self.store.get_room(room_id)
        if not room:
            raise NotFoundError("Room not found")

        state_filter = None
        type = parse_string(request, "type")

        if type:
            state_filter = StateFilter(
                types=immutabledict({type: None}),
                include_others=False,
            )

        event_ids = await self._storage_controllers.state.get_current_state_ids(
            room_id, state_filter
        )
        events = await self.store.get_events(event_ids.values())
        now = self.clock.time_msec()
        room_state = await self._event_serializer.serialize_events(events.values(), now)
        ret = {"state": room_state}

        return HTTPStatus.OK, ret


class JoinRoomAliasServlet(ResolveRoomIdMixin, RestServlet):
    PATTERNS = admin_patterns("/join/(?P<room_identifier>[^/]*)$")

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)
        self.auth = hs.get_auth()
        self.admin_handler = hs.get_admin_handler()
        self.store = hs.get_datastores().main
        self._storage_controllers = hs.get_storage_controllers()
        self.is_mine = hs.is_mine

    async def on_POST(
        self, request: SynapseRequest, room_identifier: str
    ) -> Tuple[int, JsonDict]:
        # This will always be set by the time Twisted calls us.
        assert request.args is not None

        requester = await self.auth.get_user_by_req(request)
        await assert_user_is_admin(self.auth, requester)

        content = parse_json_object_from_request(request)

        assert_params_in_dict(content, ["user_id"])
        target_user = UserID.from_string(content["user_id"])

        if not self.is_mine(target_user):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "This endpoint can only be used with local users",
            )

        if not await self.admin_handler.get_user(target_user):
            raise NotFoundError("User not found")

        # Get the room ID from the identifier.
        try:
            remote_room_hosts: Optional[List[str]] = [
                x.decode("ascii") for x in request.args[b"server_name"]
            ]
        except Exception:
            remote_room_hosts = None
        room_id, remote_room_hosts = await self.resolve_room_id(
            room_identifier, remote_room_hosts
        )

        fake_requester = create_requester(
            target_user, authenticated_entity=requester.authenticated_entity
        )

        # send invite if room has "JoinRules.INVITE"
        join_rules_event = (
            await self._storage_controllers.state.get_current_state_event(
                room_id, EventTypes.JoinRules, ""
            )
        )
        if join_rules_event:
            if not (join_rules_event.content.get("join_rule") == JoinRules.PUBLIC):
                # update_membership with an action of "invite" can raise a
                # ShadowBanError. This is not handled since it is assumed that
                # an admin isn't going to call this API with a shadow-banned user.
                await self.room_member_handler.update_membership(
                    requester=requester,
                    target=fake_requester.user,
                    room_id=room_id,
                    action="invite",
                    remote_room_hosts=remote_room_hosts,
                    ratelimit=False,
                )

        await self.room_member_handler.update_membership(
            requester=fake_requester,
            target=fake_requester.user,
            room_id=room_id,
            action="join",
            remote_room_hosts=remote_room_hosts,
            ratelimit=False,
        )

        return HTTPStatus.OK, {"room_id": room_id}


class MakeRoomAdminRestServlet(ResolveRoomIdMixin, RestServlet):
    """Allows a server admin to get power in a room if a local user has power in
    a room. Will also invite the user if they're not in the room and it's a
    private room. Can specify another user (rather than the admin user) to be
    granted power, e.g.:

        POST/_synapse/admin/v1/rooms/<room_id_or_alias>/make_room_admin
        {
            "user_id": "@foo:example.com"
        }
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_identifier>[^/]*)/make_room_admin$")

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main
        self._state_storage_controller = hs.get_storage_controllers().state
        self.event_creation_handler = hs.get_event_creation_handler()
        self.state_handler = hs.get_state_handler()
        self.is_mine_id = hs.is_mine_id

    async def on_POST(
        self, request: SynapseRequest, room_identifier: str
    ) -> Tuple[int, JsonDict]:
        requester = await self.auth.get_user_by_req(request)
        await assert_user_is_admin(self.auth, requester)
        content = parse_json_object_from_request(request, allow_empty_body=True)

        room_id, _ = await self.resolve_room_id(room_identifier)

        # Which user to grant room admin rights to.
        user_to_add = content.get("user_id", requester.user.to_string())

        # Figure out which local users currently have power in the room, if any.
        filtered_room_state = await self._state_storage_controller.get_current_state(
            room_id,
            StateFilter.from_types(
                [
                    (EventTypes.Create, ""),
                    (EventTypes.PowerLevels, ""),
                    (EventTypes.JoinRules, ""),
                    (EventTypes.Member, user_to_add),
                ]
            ),
        )
        if not filtered_room_state:
            raise SynapseError(HTTPStatus.BAD_REQUEST, "Server not in room")

        create_event = filtered_room_state[(EventTypes.Create, "")]
        power_levels = filtered_room_state.get((EventTypes.PowerLevels, ""))

        if power_levels is not None:
            # We pick the local user with the highest power.
            user_power = power_levels.content.get("users", {})
            admin_users = [
                user_id for user_id in user_power if self.is_mine_id(user_id)
            ]
            admin_users.sort(key=lambda user: user_power[user])

            if create_event.room_version.msc4289_creator_power_enabled:
                creators = create_event.content.get("additional_creators", []) + [
                    create_event.sender
                ]
                for creator in creators:
                    if self.is_mine_id(creator):
                        # include the creator as they won't be in the PL users map.
                        admin_users.append(creator)

            if not admin_users:
                raise SynapseError(
                    HTTPStatus.BAD_REQUEST, "No local admin user in room"
                )

            admin_user_id = None

            for admin_user in reversed(admin_users):
                (
                    current_membership_type,
                    _,
                ) = await self.store.get_local_current_membership_for_user_in_room(
                    admin_user, room_id
                )
                if current_membership_type == "join":
                    admin_user_id = admin_user
                    break

            if not admin_user_id:
                raise SynapseError(
                    HTTPStatus.BAD_REQUEST,
                    "No local admin user in room",
                )

            pl_content = power_levels.content
        else:
            # If there is no power level events then the creator has rights.
            pl_content = {}
            admin_user_id = create_event.sender
            if not self.is_mine_id(admin_user_id):
                raise SynapseError(
                    HTTPStatus.BAD_REQUEST,
                    "No local admin user in room",
                )

        # Grant the user power equal to the room admin by attempting to send an
        # updated power level event.
        new_pl_content = dict(pl_content)
        new_pl_content["users"] = dict(pl_content.get("users", {}))
        # give the new user the same PL as the admin, default to 100 in case there is no PL event.
        # This means in v12+ rooms we get PL100 if the creator promotes us.
        new_pl_content["users"][user_to_add] = new_pl_content["users"].get(
            admin_user_id, 100
        )

        fake_requester = create_requester(
            admin_user_id,
            authenticated_entity=requester.authenticated_entity,
        )

        try:
            await self.event_creation_handler.create_and_send_nonmember_event(
                fake_requester,
                event_dict={
                    "content": new_pl_content,
                    "sender": admin_user_id,
                    "type": EventTypes.PowerLevels,
                    "state_key": "",
                    "room_id": room_id,
                },
            )
        except AuthError:
            # The admin user we found turned out not to have enough power.
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "No local admin user in room with power to update power levels.",
            )

        # Now we check if the user we're granting admin rights to is already in
        # the room. If not and it's not a public room we invite them.
        member_event = filtered_room_state.get((EventTypes.Member, user_to_add))
        is_joined = False
        if member_event:
            is_joined = member_event.content["membership"] in (
                Membership.JOIN,
                Membership.INVITE,
            )

        if is_joined:
            return HTTPStatus.OK, {}

        join_rules = filtered_room_state.get((EventTypes.JoinRules, ""))
        is_public = False
        if join_rules:
            is_public = join_rules.content.get("join_rule") == JoinRules.PUBLIC

        if is_public:
            return HTTPStatus.OK, {}

        await self.room_member_handler.update_membership(
            fake_requester,
            target=UserID.from_string(user_to_add),
            room_id=room_id,
            action=Membership.INVITE,
        )

        return HTTPStatus.OK, {}


class ForwardExtremitiesRestServlet(ResolveRoomIdMixin, RestServlet):
    """Allows a server admin to get or clear forward extremities.

    Clearing does not require restarting the server.

        Clear forward extremities:
        DELETE /_synapse/admin/v1/rooms/<room_id_or_alias>/forward_extremities

        Get forward_extremities:
        GET /_synapse/admin/v1/rooms/<room_id_or_alias>/forward_extremities
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_identifier>[^/]*)/forward_extremities$")

    def __init__(self, hs: "HomeServer"):
        super().__init__(hs)
        self.auth = hs.get_auth()
        self.store = hs.get_datastores().main

    async def on_DELETE(
        self, request: SynapseRequest, room_identifier: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        room_id, _ = await self.resolve_room_id(room_identifier)

        deleted_count = await self.store.delete_forward_extremities_for_room(room_id)
        return HTTPStatus.OK, {"deleted": deleted_count}

    async def on_GET(
        self, request: SynapseRequest, room_identifier: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self.auth, request)

        room_id, _ = await self.resolve_room_id(room_identifier)

        extremities = await self.store.get_forward_extremities_for_room(room_id)
        result = [
            {
                "event_id": ex[0],
                "state_group": ex[1],
                "depth": ex[2],
                "received_ts": ex[3],
            }
            for ex in extremities
        ]

        return HTTPStatus.OK, {"count": len(extremities), "results": result}


class RoomEventContextServlet(RestServlet):
    """
    Provide the context for an event.
    This API is designed to be used when system administrators wish to look at
    an abuse report and understand what happened during and immediately prior
    to this event.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/context/(?P<event_id>[^/]*)$")

    def __init__(self, hs: "HomeServer"):
        super().__init__()
        self._hs = hs
        self.clock = hs.get_clock()
        self.room_context_handler = hs.get_room_context_handler()
        self._event_serializer = hs.get_event_client_serializer()
        self.auth = hs.get_auth()

    async def on_GET(
        self, request: SynapseRequest, room_id: str, event_id: str
    ) -> Tuple[int, JsonDict]:
        requester = await self.auth.get_user_by_req(request, allow_guest=False)
        await assert_user_is_admin(self.auth, requester)

        limit = parse_integer(request, "limit", default=10)

        # picking the API shape for symmetry with /messages
        filter_json = parse_json(request, "filter", encoding="utf-8")
        event_filter = Filter(self._hs, filter_json) if filter_json else None

        event_context = await self.room_context_handler.get_event_context(
            requester,
            room_id,
            event_id,
            limit,
            event_filter,
            use_admin_priviledge=True,
        )

        if not event_context:
            raise SynapseError(
                HTTPStatus.NOT_FOUND, "Event not found.", errcode=Codes.NOT_FOUND
            )

        time_now = self.clock.time_msec()
        results = {
            "events_before": await self._event_serializer.serialize_events(
                event_context.events_before,
                time_now,
                bundle_aggregations=event_context.aggregations,
            ),
            "event": await self._event_serializer.serialize_event(
                event_context.event,
                time_now,
                bundle_aggregations=event_context.aggregations,
            ),
            "events_after": await self._event_serializer.serialize_events(
                event_context.events_after,
                time_now,
                bundle_aggregations=event_context.aggregations,
            ),
            "state": await self._event_serializer.serialize_events(
                event_context.state, time_now
            ),
            "start": event_context.start,
            "end": event_context.end,
        }

        return HTTPStatus.OK, results


class BlockRoomRestServlet(RestServlet):
    """
    Manage blocking of rooms.
    On PUT: Add or remove a room from blocking list.
    On GET: Get blocking status of room and user who has blocked this room.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/block$")

    def __init__(self, hs: "HomeServer"):
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self._auth, request)

        if not RoomID.is_valid(room_id):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "%s is not a legal room ID" % (room_id,)
            )

        blocked_by = await self._store.room_is_blocked_by(room_id)
        # Test `not None` if `user_id` is an empty string
        # if someone add manually an entry in database
        if blocked_by is not None:
            response = {"block": True, "user_id": blocked_by}
        else:
            response = {"block": False}

        return HTTPStatus.OK, response

    async def on_PUT(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        requester = await self._auth.get_user_by_req(request)
        await assert_user_is_admin(self._auth, requester)

        content = parse_json_object_from_request(request)

        if not RoomID.is_valid(room_id):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "%s is not a legal room ID" % (room_id,)
            )

        assert_params_in_dict(content, ["block"])
        block = content.get("block")
        if not isinstance(block, bool):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST,
                "Param 'block' must be a boolean.",
                Codes.BAD_JSON,
            )

        if block:
            await self._store.block_room(room_id, requester.user.to_string())
        else:
            await self._store.unblock_room(room_id)

        return HTTPStatus.OK, {"block": block}


class RoomMessagesRestServlet(RestServlet):
    """
    Get messages list of a room.
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/messages$")

    def __init__(self, hs: "HomeServer"):
        self._hs = hs
        self._clock = hs.get_clock()
        self._pagination_handler = hs.get_pagination_handler()
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        requester = await self._auth.get_user_by_req(request)
        await assert_user_is_admin(self._auth, requester)

        pagination_config = await PaginationConfig.from_request(
            self._store, request, default_limit=10
        )
        # Twisted will have processed the args by now.
        assert request.args is not None

        filter_json = parse_json(request, "filter", encoding="utf-8")
        event_filter = Filter(self._hs, filter_json) if filter_json else None

        as_client_event = b"raw" not in request.args
        if (
            event_filter
            and event_filter.filter_json.get("event_format", "client") == "federation"
        ):
            as_client_event = False

        msgs = await self._pagination_handler.get_messages(
            room_id=room_id,
            requester=requester,
            pagin_config=pagination_config,
            as_client_event=as_client_event,
            event_filter=event_filter,
            use_admin_priviledge=True,
        )

        return HTTPStatus.OK, msgs


class RoomTimestampToEventRestServlet(RestServlet):
    """
    API endpoint to fetch the `event_id` of the closest event to the given
    timestamp (`ts` query parameter) in the given direction (`dir` query
    parameter).

    Useful for cases like jump to date so you can start paginating messages from
    a given date in the archive.

    `ts` is a timestamp in milliseconds where we will find the closest event in
    the given direction.

    `dir` can be `f` or `b` to indicate forwards and backwards in time from the
    given timestamp.

    GET /_synapse/admin/v1/rooms/<roomID>/timestamp_to_event?ts=<timestamp>&dir=<direction>
    {
        "event_id": ...
    }
    """

    PATTERNS = admin_patterns("/rooms/(?P<room_id>[^/]*)/timestamp_to_event$")

    def __init__(self, hs: "HomeServer"):
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main
        self._timestamp_lookup_handler = hs.get_timestamp_lookup_handler()

    async def on_GET(
        self, request: SynapseRequest, room_id: str
    ) -> Tuple[int, JsonDict]:
        requester = await self._auth.get_user_by_req(request)
        await assert_user_is_admin(self._auth, requester)

        timestamp = parse_integer(request, "ts", required=True)
        direction = parse_enum(request, "dir", Direction, default=Direction.FORWARDS)

        (
            event_id,
            origin_server_ts,
        ) = await self._timestamp_lookup_handler.get_event_for_timestamp(
            requester, room_id, timestamp, direction
        )

        return HTTPStatus.OK, {
            "event_id": event_id,
            "origin_server_ts": origin_server_ts,
        }


class BulkEventInjectionServlet(RestServlet):
    """Admin endpoint for bulk historical event injection for disaster recovery.
    
    This simplified implementation reuses existing Synapse code wherever possible:
    - Uses federation_event_handler for event persistence
    - Leverages existing state resolution
    - Reuses room creation service
    - Lets Synapse handle all database operations
    
    POST /_synapse/admin/v1/bulk_inject
    {
        "events": [
            {
                "event_id": "$eventid:server.com",
                "type": "m.room.message",
                "sender": "@user:server.com",
                "content": {"msgtype": "m.text", "body": "message"},
                "origin_server_ts": 1234567890000,
                "room_id": "!ABCDEFGHIJKLMNOPQR:server.com",
                "auth_events": [],  # Optional: will be auto-populated if missing
                "prev_events": [],  # Optional: will be auto-populated if missing
                "depth": 123,       # Optional: will be auto-calculated if missing
                "state_key": null   # For state events
            }
        ]
    }
    """

    PATTERNS = admin_patterns("/bulk_inject$")

    def __init__(self, hs: "HomeServer"):
        self._hs = hs
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main
        self._federation_event_handler = hs.get_federation_event_handler()
        self._room_creation_handler = hs.get_room_creation_handler()
        self._state_handler = hs.get_state_handler()
        self._event_creation_handler = hs.get_event_creation_handler()
        self._storage_controllers = hs.get_storage_controllers()

    async def on_POST(self, request: SynapseRequest) -> Tuple[int, JsonDict]:
        """Handle bulk event injection request."""
        await assert_requester_is_admin(self._auth, request)

        content = parse_json_object_from_request(request)

        # Validate request
        if "events" not in content:
            raise SynapseError(400, "Missing 'events' field", Codes.BAD_JSON)

        events_data = content["events"]
        if not isinstance(events_data, list):
            raise SynapseError(400, "'events' must be a list", Codes.BAD_JSON)

        if not events_data:
            return 200, {
                "injected_events": 0,
                "failed_events": 0,
                "errors": [],
            }

        # Group events by room
        events_by_room: Dict[str, List[JsonDict]] = {}
        for event_dict in events_data:
            room_id = event_dict.get("room_id")
            if room_id:
                events_by_room.setdefault(room_id, []).append(event_dict)

        logger.info(
            "Processing %d events across %d rooms",
            len(events_data),
            len(events_by_room)
        )

        # Process each room
        total_injected = 0
        total_failed = 0
        all_errors = []
        event_id_mapping = {}

        for room_id, room_events in events_by_room.items():
            try:
                # Check if room exists, create if needed
                await self._ensure_room_exists(room_id, room_events)
                
                # Process events for this room
                injected, failed, errors, mapping = await self._process_room_events(
                    room_id, room_events
                )
                
                total_injected += injected
                total_failed += failed
                all_errors.extend(errors)
                event_id_mapping.update(mapping)
                
            except Exception as e:
                logger.exception("Failed to process room %s", room_id)
                total_failed += len(room_events)
                all_errors.append({
                    "room_id": room_id,
                    "error": str(e)
                })

        response = {
            "injected_events": total_injected,
            "failed_events": total_failed,
            "errors": all_errors,
        }
        
        if event_id_mapping:
            response["event_id_mapping"] = event_id_mapping

        return 200, response

    async def _ensure_room_exists(
        self, room_id: str, events: List[JsonDict]
    ) -> None:
        """Ensure room exists, creating it if necessary."""
        try:
            await self._store.get_room_version(room_id)
            return  # Room exists
        except:
            pass  # Room doesn't exist

        # Look for create event
        create_event_dict = None
        logger.info(
            "Looking for create event in room %s among %d events. Event types: %s",
            room_id,
            len(events),
            [e.get("type") for e in events[:10]]  # First 10 event types
        )
        for event in events:
            if event.get("type") == EventTypes.Create and event.get("state_key") == "":
                create_event_dict = event
                logger.info("Found create event for room %s", room_id)
                break

        if not create_event_dict:
            logger.warning(
                "No create event found for room %s. Events have these types: %s",
                room_id,
                [e.get("type") for e in events]
            )
            raise SynapseError(
                400,
                f"Room {room_id} does not exist and no create event provided"
            )

        # Extract room version
        content = create_event_dict.get("content", {})
        # Default to v1 for rooms without explicit version (original Matrix spec)
        room_version_id = content.get("room_version", RoomVersions.V1.identifier)
        
        try:
            room_version = getattr(RoomVersions, f"V{room_version_id}")
        except AttributeError:
            logger.warning(
                "Unknown room version %s, using V10",
                room_version_id
            )
            room_version = RoomVersions.V10

        # Create room in database
        creator = create_event_dict.get("sender")
        if not creator:
            raise SynapseError(400, "Create event missing sender")

        await self._store.store_room(
            room_id=room_id,
            room_creator_user_id=creator,
            is_public=False,
            room_version=room_version,
        )
        
        logger.info(
            "Created room %s with version %s",
            room_id,
            room_version.identifier
        )

    async def _process_room_events(
        self, room_id: str, events_data: List[JsonDict]
    ) -> Tuple[int, int, List[JsonDict], Dict[str, str]]:
        """Process events for a single room using existing Synapse code."""

        # Get room version
        room_version = await self._store.get_room_version(room_id)

        # Convert to EventBase objects
        # Note: We pass an empty state_map initially since room doesn't exist yet
        # Events will be processed sequentially and state will be queried from DB as we go
        events = []
        errors = []
        event_id_mapping = {}
        state_map: Dict[Tuple[str, str], str] = {}  # Empty initially
        
        for idx, event_dict in enumerate(events_data):
            try:
                logger.debug("Processing event %d/%d", idx + 1, len(events_data))
                
                # Track original event_id for mapping BEFORE prepare removes it
                original_event_id = event_dict.get("event_id", "")
                
                logger.info("Processing event %s", original_event_id or "(no id)")
                
                # Auto-populate missing fields
                event_dict = await self._prepare_event_dict(
                    room_id, event_dict, room_version, state_map
                )
                
                # The format checking is now done in _prepare_event_dict
                # No need to check again here
                
                # Create event
                logger.info("Creating event from dict with keys: %s", list(event_dict.keys()))
                event = make_event_from_dict(event_dict, room_version)
                logger.info("Created event %s", event.event_id)

                # Validate event ID - MUST ALWAYS match for disaster recovery
                # For room v3+, event IDs are content-addressable. If the provided
                # cryptographic data is correct, the calculated ID will match.
                if not original_event_id:
                    raise SynapseError(
                        400,
                        "event_id is required for disaster recovery. "
                        "Use federation or database exports as data sources.",
                        Codes.BAD_JSON
                    )

                actual_event_id = event.event_id

                if original_event_id != actual_event_id:
                    # For room v3+, event IDs are content-addressable
                    # For room v1/v2, event IDs are part of the event structure
                    if room_version.event_format >= EventFormatVersions.ROOM_V3:
                        error_msg = (
                            f"Event ID mismatch: provided event_id {original_event_id} "
                            f"but calculated event_id is {actual_event_id}. "
                            f"For room version {room_version.identifier}, event IDs are content-addressable "
                            f"and calculated from the event content. This mismatch indicates the provided "
                            f"cryptographic data (hashes/signatures/auth_events/prev_events) does not match "
                            f"the event content. Ensure you're using complete events from federation or "
                            f"database exports, NOT client API endpoints. Rejecting to prevent "
                            f"federation desynchronization."
                        )
                    else:
                        error_msg = (
                            f"Event ID mismatch: provided event_id {original_event_id} "
                            f"but got event_id {actual_event_id} after event creation. "
                            f"For room version {room_version.identifier}, event IDs should be preserved as-is."
                        )
                    raise SynapseError(400, error_msg, Codes.BAD_JSON)

                logger.info(
                    "Event ID validated: %s",
                    original_event_id
                )

                # Validate room_id for room v12+ (MSC4291: room IDs as hashes)
                # For room v12+, room_id MUST be present and match the create event's event_id
                if room_version.event_format >= EventFormatVersions.ROOM_V11_HYDRA_PLUS:
                    # Check if room_id is in event_dict
                    provided_room_id = event_dict.get("room_id")
                    if not provided_room_id:
                        raise SynapseError(
                            400,
                            f"room_id is required in event JSON for room version {room_version.identifier}+. "
                            f"Room v12+ uses MSC4291 where room_id is derived from the create event's event_id. "
                            f"Synapse stores room_id as a separate database column, but the bulk injection API "
                            f"requires it in the event JSON for validation. Ensure your uploader adds room_id to "
                            f"each event.",
                            Codes.BAD_JSON
                        )

                    # For create events in room v12+, validate room_id = event_id with ! prefix
                    if event.type == EventTypes.Create and event.state_key == "":
                        expected_room_id = "!" + event.event_id[1:]  # Replace $ with !
                        if provided_room_id != expected_room_id:
                            raise SynapseError(
                                400,
                                f"Room ID mismatch for room version {room_version.identifier} create event: "
                                f"provided room_id {provided_room_id} but expected {expected_room_id} "
                                f"(derived from create event_id {event.event_id}). "
                                f"Room v12+ (MSC4291) requires room_id to be the create event's event_id "
                                f"with '!' prefix instead of '$'. Rejecting to prevent room ID desynchronization.",
                                Codes.BAD_JSON
                            )
                        logger.info(
                            "Room ID validated for v12+ create event: %s = %s with ! prefix",
                            provided_room_id,
                            event.event_id
                        )

                events.append(event)
                    
            except Exception as e:
                import traceback
                tb = traceback.format_exc()

                # Use the original event ID if available, otherwise try to get from event_dict
                error_event_id = original_event_id or event_dict.get("event_id", "unknown")

                logger.error(
                    "Failed to create event %s in room %s: %s\nTraceback:\n%s",
                    error_event_id,
                    room_id,
                    e,
                    tb
                )

                errors.append({
                    "event_id": error_event_id,
                    "error": str(e),
                    "type": type(e).__name__,
                    "traceback": tb
                })

        if not events:
            return 0, len(events_data), errors, event_id_mapping

        # Sort events with create event absolutely first, then by depth
        # This is critical: state resolution requires the create event to exist
        def event_sort_key(event):
            # Create events always first (is_create = True sorts before False)
            is_create = event.type == EventTypes.Create
            # Then by depth, then timestamp
            return (not is_create, event.depth, event.origin_server_ts)

        events.sort(key=event_sort_key)

        logger.info(
            "Processing %d events for room %s (first event: %s)",
            len(events),
            room_id,
            events[0].type if events else "none"
        )

        # CRITICAL: Before processing any events, upgrade ALL outlier forward extremities
        # These must be processed FIRST to avoid KeyError during state resolution
        # This handles the case where events from a snapshot are outliers and forward extremities
        # We loop because upgrading extremities can reveal new extremities
        event_ids_in_batch = {event.event_id for event in events}
        total_upgraded = 0
        max_iterations = 100  # Prevent infinite loops

        for iteration in range(max_iterations):
            forward_extremities = await self._store.get_forward_extremities_for_room(room_id)
            # forward_extremities is List[Tuple[event_id, state_group, depth, received_ts]]
            # Extract just the event IDs for easier checking
            forward_extremity_ids = {extremity[0] for extremity in forward_extremities}

            # Find ALL outlier forward extremities (whether in batch or not)
            outlier_extremities_to_upgrade = []
            for extremity_id in forward_extremity_ids:
                extremity_event = await self._store.get_event(extremity_id, allow_none=True)
                if extremity_event and extremity_event.internal_metadata.is_outlier():
                    outlier_extremities_to_upgrade.append(extremity_event)

            if not outlier_extremities_to_upgrade:
                # No more outlier extremities to upgrade
                if total_upgraded > 0:
                    logger.warning(
                        "Finished upgrading %d outlier forward extremities in %d iterations for room %s",
                        total_upgraded,
                        iteration,
                        room_id
                    )
                break

            logger.warning(
                "Iteration %d: Found %d outlier forward extremities to upgrade in room %s: %s",
                iteration + 1,
                len(outlier_extremities_to_upgrade),
                room_id,
                [e.event_id for e in outlier_extremities_to_upgrade]
            )

            # Upgrade these outlier extremities
            for extremity_event in outlier_extremities_to_upgrade:
                logger.warning(
                    "Upgrading outlier forward extremity %s",
                    extremity_event.event_id
                )
                try:
                    context = await self._state_handler.compute_event_context(extremity_event)
                    await self._federation_event_handler.persist_events_and_notify(
                        room_id,
                        [(extremity_event, context)],
                        backfilled=False
                    )
                    logger.warning(
                        "Successfully upgraded outlier extremity %s",
                        extremity_event.event_id
                    )
                    total_upgraded += 1
                except Exception as e:
                    logger.error(
                        "Failed to upgrade outlier extremity %s: %s",
                        extremity_event.event_id,
                        e
                    )
                    raise

        if iteration >= max_iterations - 1:
            raise Exception(
                f"Hit maximum iterations ({max_iterations}) while upgrading outlier extremities in room {room_id}"
            )

        # Get final forward extremities after all upgrades
        forward_extremities = await self._store.get_forward_extremities_for_room(room_id)
        forward_extremity_ids = {extremity[0] for extremity in forward_extremities}

        outlier_extremity_events = []
        non_extremity_events = []

        # Now reorder events in this batch to process outlier extremities first
        for event in events:
            if event.event_id in forward_extremity_ids:
                # Check if it's an outlier
                existing = await self._store.get_event(event.event_id, allow_none=True)
                if existing and existing.internal_metadata.is_outlier():
                    outlier_extremity_events.append(event)
                    logger.warning(
                        "Event %s is an outlier forward extremity, will process first",
                        event.event_id
                    )
                else:
                    non_extremity_events.append(event)
            else:
                non_extremity_events.append(event)

        # Process outlier extremities first, then everything else
        events_to_process = outlier_extremity_events + non_extremity_events

        if outlier_extremity_events:
            logger.warning(
                "Reordered batch to process %d outlier forward extremities first in room %s",
                len(outlier_extremity_events),
                room_id
            )

        # Process events using federation handler
        # This handles all the complexity of state resolution, persistence, etc.
        # CRITICAL: Process events SEQUENTIALLY, not in batches
        # State resolution for later events depends on earlier events (especially create) being persisted first
        successfully_processed = 0

        for event in events_to_process:
            try:
                # Check if event already exists (for idempotent re-uploads)
                # This avoids "No forward extremities left" errors and database inconsistency
                existing_event = await self._store.get_event(event.event_id, allow_none=True)
                if existing_event:
                    # If event exists as an outlier (e.g., from backfill or previous incomplete upload),
                    # we need to re-process it to upgrade it to a non-outlier with proper state.
                    # This handles the case where a room was partially imported from a snapshot
                    # and we're now doing a complete disaster recovery import.
                    if existing_event.internal_metadata.is_outlier():
                        logger.warning(
                            "Event %s already exists as outlier in room %s, re-processing to upgrade with state",
                            event.event_id,
                            room_id
                        )
                        # Continue processing - Synapse's persist layer will handle the outlier upgrade
                    else:
                        # Event already exists as non-outlier, skip
                        logger.warning(
                            "Event %s already exists in room %s, skipping (idempotent re-upload)",
                            event.event_id,
                            room_id
                        )
                        successfully_processed += 1
                        continue

                # Compute context BEFORE persisting
                # This allows state resolution to see previously persisted events
                context = await self._state_handler.compute_event_context(event)

                # Persist ONE event at a time to ensure sequential processing
                # This is critical for disaster recovery where we're rebuilding room state from scratch
                await self._federation_event_handler.persist_events_and_notify(
                    room_id,
                    [(event, context)],
                    backfilled=False  # Use positive stream ordering for visibility
                )

                # IMPORTANT: Wait for persistence to complete before processing next event
                # This ensures the create event is fully visible before processing dependent events

                # Verify if outlier was upgraded
                if existing_event and existing_event.internal_metadata.is_outlier():
                    updated_event = await self._store.get_event(event.event_id, allow_none=True)
                    if updated_event and updated_event.internal_metadata.is_outlier():
                        logger.error(
                            "Event %s is STILL an outlier after persist! Outlier upgrade failed.",
                            event.event_id
                        )
                    else:
                        logger.warning(
                            "Event %s successfully upgraded from outlier to non-outlier",
                            event.event_id
                        )

                successfully_processed += 1
                logger.debug("Successfully processed event %s", event.event_id)
                
            except Exception as e:
                import traceback
                tb = traceback.format_exc()

                # Use logger.error so it always appears in logs
                logger.error(
                    "Failed to process event %s in room %s: %s\nTraceback:\n%s",
                    event.event_id,
                    room_id,
                    e,
                    tb
                )

                errors.append({
                    "event_id": event.event_id,
                    "error": str(e),
                    "type": type(e).__name__,
                    "traceback": tb
                })

        failed_count = len(errors)  # Only actual errors count as failures

        if failed_count > 0:
            logger.error(
                "Room %s: %d/%d events failed to process. Errors: %s",
                room_id,
                failed_count,
                len(events),
                [{"event_id": e["event_id"], "error": e["error"]} for e in errors]
            )

        logger.info(
            "Processed %d/%d events successfully for room %s (failed: %d)",
            successfully_processed,
            len(events),
            room_id,
            failed_count
        )

        # CRITICAL: Rebuild forward extremities and current_state_events from the imported events
        # This is necessary because bulk injection doesn't automatically update these tables
        if successfully_processed > 0:
            try:
                # First, recalculate forward extremities
                # Forward extremities are events that have no children (not referenced in prev_events)
                logger.info(
                    "Recalculating forward extremities for room %s after bulk injection",
                    room_id
                )
                await self._recalculate_forward_extremities(room_id)
                logger.info(
                    "Successfully recalculated forward extremities for room %s",
                    room_id
                )

                # Then rebuild current_state_events based on the new forward extremities
                logger.info(
                    "Rebuilding current_state_events for room %s after bulk injection",
                    room_id
                )
                await self._storage_controllers.persistence.update_current_state(room_id)
                logger.info(
                    "Successfully rebuilt current_state_events for room %s",
                    room_id
                )
            except Exception as e:
                logger.error(
                    "Failed to rebuild room state for room %s: %s",
                    room_id,
                    e,
                    exc_info=True
                )
                # Re-raise the exception - this is a critical failure
                # Without correct forward extremities and current state, the room is broken
                raise

        return successfully_processed, failed_count, errors, event_id_mapping

    async def _recalculate_forward_extremities(self, room_id: str) -> None:
        """Recalculate forward extremities for a room after bulk event injection.

        Forward extremities are events that have no children - i.e., events that are
        not referenced in any other event's prev_events.

        This is necessary because bulk injection bypasses normal event persistence
        which would update forward extremities incrementally.
        """

        def _recalculate_forward_extremities_txn(txn):
            # Forward extremities are ALWAYS among the most recent events in a room.
            # For efficiency, we only examine the last 1000 events by stream_ordering.
            # This handles even complex DAGs with many branches while avoiding
            # loading millions of events into memory.

            # Get the most recent events (candidates for forward extremities)
            sql = """
                SELECT e.event_id
                FROM events e
                WHERE e.room_id = ?
                ORDER BY e.stream_ordering DESC
                LIMIT 1000
            """
            txn.execute(sql, (room_id,))
            candidate_event_ids = {row[0] for row in txn.fetchall()}

            # Get the JSON for these recent events to extract their prev_events
            sql = """
                SELECT ej.event_id, ej.json
                FROM event_json ej
                WHERE ej.event_id = ANY(?)
            """ if self._store.database_engine.supports_using_any_list else """
                SELECT ej.event_id, ej.json
                FROM event_json ej
                JOIN events e ON e.event_id = ej.event_id
                WHERE e.room_id = ?
                ORDER BY e.stream_ordering DESC
                LIMIT 1000
            """

            if self._store.database_engine.supports_using_any_list:
                txn.execute(sql, (list(candidate_event_ids),))
            else:
                txn.execute(sql, (room_id,))

            # Find which candidate events are referenced in prev_events
            referenced_events = set()
            for event_id, json_str in txn.fetchall():
                try:
                    import json
                    event_json = json.loads(json_str)
                    prev_events = event_json.get("prev_events", [])

                    # prev_events can be either ["$event_id"] or [["$event_id", {}]]
                    for prev in prev_events:
                        if isinstance(prev, list):
                            referenced_events.add(prev[0])
                        else:
                            referenced_events.add(prev)
                except Exception as e:
                    logger.warning(
                        "Failed to parse prev_events from event %s: %s",
                        event_id,
                        e
                    )

            # Forward extremities are candidates NOT referenced in anyone's prev_events
            new_extremities = candidate_event_ids - referenced_events

            if not new_extremities:
                logger.warning(
                    "No forward extremities found for room %s after recalculation. "
                    "This might indicate an issue with the event DAG.",
                    room_id
                )
                return

            logger.info(
                "Found %d forward extremities for room %s: %s",
                len(new_extremities),
                room_id,
                new_extremities
            )

            # Delete existing forward extremities
            self._store.db_pool.simple_delete_txn(
                txn,
                table="event_forward_extremities",
                keyvalues={"room_id": room_id}
            )

            # Insert new forward extremities
            self._store.db_pool.simple_insert_many_txn(
                txn,
                table="event_forward_extremities",
                keys=("event_id", "room_id"),
                values=[(event_id, room_id) for event_id in new_extremities]
            )

            # Invalidate get_latest_event_ids_in_room cache
            self._store._invalidate_cache_and_stream(
                txn,
                self._store.get_latest_event_ids_in_room,
                (room_id,)
            )

        await self._store.db_pool.runInteraction(
            "recalculate_forward_extremities",
            _recalculate_forward_extremities_txn
        )

    async def _prepare_event_dict(
        self, room_id: str, event_dict: JsonDict, room_version, state_map: Dict[Tuple[str, str], str]
    ) -> JsonDict:
        """Prepare event dict for disaster recovery.

        For disaster recovery, we require COMPLETE events with all cryptographic data.
        This ensures event IDs are preserved and federation consistency is maintained.

        Args:
            room_id: The room ID
            event_dict: The event dictionary (must be complete)
            room_version: The room version
            state_map: Map of (type, state_key) -> event_id for tracking state during import
        """
        # Make a copy to avoid modifying the original
        event_dict = dict(event_dict)

        # Validate required fields for disaster recovery
        # All these fields are required by federation PDU format for ALL room versions
        required = ["type", "sender", "content", "origin_server_ts", "room_id", "event_id",
                    "auth_events", "prev_events", "depth", "hashes", "signatures"]
        missing = [f for f in required if f not in event_dict]
        if missing:
            raise SynapseError(
                400,
                f"Incomplete event data - missing required fields: {missing}. "
                f"For disaster recovery, all events must include complete PDU data: "
                f"{', '.join(required)}. "
                f"Use federation or database exports as data sources, "
                f"NOT client API endpoints (/messages, /sync) which lack these fields.",
                Codes.BAD_JSON
            )

        # Convert format if needed (v3+ uses simple lists, not tuples)
        if room_version.event_format >= EventFormatVersions.ROOM_V3:
            if event_dict.get("auth_events") and isinstance(event_dict["auth_events"][0], (list, tuple)):
                event_dict["auth_events"] = [e[0] for e in event_dict["auth_events"]]

            if event_dict.get("prev_events") and isinstance(event_dict["prev_events"][0], (list, tuple)):
                event_dict["prev_events"] = [e[0] for e in event_dict["prev_events"]]

            # Remove event_id before event creation (required by FrozenEventV2 assertion)
            # For room v3+, event IDs are content-addressable - they're calculated from
            # the event's content. If the provided data is complete (including hashes,
            # signatures), the recalculated ID will match the original. This is validated
            # after event creation to ensure no desynchronization occurs.
            event_dict.pop("event_id", None)
        # For room v1/v2, keep event_id in the dict (it's part of the event structure)

        return event_dict

    async def _get_auth_events_for_event(
        self,
        room_id: str,
        event_type: str,
        state_key: Optional[str],
        sender: str,
        state_map: Dict[Tuple[str, str], str]
    ) -> List[str]:
        """Get required auth events for an event type.

        Args:
            room_id: The room ID
            event_type: The event type
            state_key: The state key (None for non-state events)
            sender: The event sender
            state_map: Current state map from bulk import (used for disaster recovery)

        Returns:
            List of auth event IDs
        """

        # Try to get state from state_map first (for disaster recovery)
        # Fall back to database query if state_map is empty (normal operation)
        if state_map:
            state_ids = state_map
        else:
            # Get current state from database
            state_ids = await self._storage_controllers.state.get_current_state_ids(
                room_id
            )

        auth_event_ids = []

        # Always need create event
        create_id = state_ids.get((EventTypes.Create, ""))
        if create_id:
            auth_event_ids.append(create_id)

        # Always need sender's membership
        sender_member = state_ids.get((EventTypes.Member, sender))
        if sender_member:
            auth_event_ids.append(sender_member)

        # Always need power levels
        power_levels = state_ids.get((EventTypes.PowerLevels, ""))
        if power_levels:
            auth_event_ids.append(power_levels)

        # For member events, need join rules
        if event_type == EventTypes.Member:
            join_rules = state_ids.get((EventTypes.JoinRules, ""))
            if join_rules:
                auth_event_ids.append(join_rules)

        # For state events, might need the previous state
        if state_key is not None:
            prev_state = state_ids.get((event_type, state_key))
            if prev_state and prev_state not in auth_event_ids:
                auth_event_ids.append(prev_state)

        return auth_event_ids