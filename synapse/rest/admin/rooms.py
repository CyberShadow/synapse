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
from synapse.events import EventBase
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
    """Admin endpoint for bulk historical event injection

    This endpoint allows injecting batches of historical events for disaster
    recovery purposes. Events are processed at the federation level to preserve
    original timestamps and maintain historical integrity.

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
                "auth_events": ["$auth1:server.com", "$auth2:server.com"],
                "prev_events": ["$prev1:server.com"],
                "depth": 123,  // Optional: auto-calculated from prev_events if not provided
                "state_key": null
            }
        ]
    }
    
    Note: Events are always injected with positive stream ordering to ensure
    proper membership tracking and visibility in room timelines. This is optimal
    for disaster recovery scenarios where you want to restore accessible history.
    
    depth (optional): Event depth for topological ordering. If not provided,
    automatically calculated based on prev_events for proper DAG ordering.
    """

    PATTERNS = admin_patterns("/bulk_inject$")

    def __init__(self, hs: "HomeServer"):
        self._hs = hs
        self._auth = hs.get_auth()
        self._store = hs.get_datastores().main
        self._federation_event_handler = hs.get_federation_event_handler()
        self._storage_controllers = hs.get_storage_controllers()
        self._state_storage = self._storage_controllers.state
        self._state_handler = hs.get_state_handler()

    async def on_POST(self, request: SynapseRequest) -> Tuple[int, JsonDict]:
        await assert_requester_is_admin(self._auth, request)

        content = parse_json_object_from_request(request)

        # Validate request format
        if "events" not in content:
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "Missing 'events' field", Codes.BAD_JSON
            )

        events_data = content["events"]
        if not isinstance(events_data, list):
            raise SynapseError(
                HTTPStatus.BAD_REQUEST, "'events' must be a list", Codes.BAD_JSON
            )

        # Always use non-backfilled events for disaster recovery
        # This ensures proper membership tracking and event visibility
        mark_as_backfilled = False

        if not events_data:
            return HTTPStatus.OK, {
                "injected_events": 0,
                "failed_events": 0,
                "errors": [],
            }

        # Group events by room to process them efficiently
        events_by_room: Dict[str, List[JsonDict]] = {}
        for event_dict in events_data:
            if not isinstance(event_dict, dict):
                continue

            room_id = event_dict.get("room_id")
            if not room_id:
                continue

            if room_id not in events_by_room:
                events_by_room[room_id] = []
            events_by_room[room_id].append(event_dict)
        
        logger.info(f"Processing {len(events_data)} events across {len(events_by_room)} rooms")

        total_injected = 0
        total_failed = 0
        all_errors = []
        event_id_mapping = {}  # original_event_id -> computed_event_id

        # Process events room by room
        for room_id, room_events in events_by_room.items():
            try:
                injected, failed, errors, room_mapping = await self._process_room_events(
                    room_id, room_events, mark_as_backfilled
                )
                total_injected += injected
                total_failed += failed
                all_errors.extend(errors)
                event_id_mapping.update(room_mapping)
            except Exception as e:
                logger.exception("Failed to process events for room %s", room_id)
                total_failed += len(room_events)
                all_errors.append(
                    {
                        "room_id": room_id,
                        "error": f"Failed to process room events: {str(e)}",
                    }
                )

        response = {
            "injected_events": total_injected,
            "failed_events": total_failed,
            "errors": all_errors,
        }
        
        # Include event_id mapping if any events were processed
        if event_id_mapping:
            response["event_id_mapping"] = event_id_mapping

        return HTTPStatus.OK, response

    async def _process_room_events(
        self, room_id: str, events_data: List[JsonDict], mark_as_backfilled: bool
    ) -> Tuple[int, int, List[JsonDict], Dict[str, str]]:
        """Process events for a single room"""
        from synapse.events import make_event_from_dict
        from synapse.events.snapshot import EventContext
        
        logger.info(
            "_process_room_events called for room %s with %d events, mark_as_backfilled=%s", 
            room_id, len(events_data), mark_as_backfilled
        )

        # Check if room exists and get room version
        try:
            room_version = await self._store.get_room_version(room_id)
        except Exception as e:
            logger.info("Room %s not found, checking for m.room.create event", room_id)
            
            # Look for m.room.create event to auto-create the room
            create_event = None
            for event_dict in events_data:
                if (event_dict.get("type") == EventTypes.Create and 
                    event_dict.get("state_key") == ""):
                    create_event = event_dict
                    break
            
            if create_event is None:
                logger.error("Room %s not found and no m.room.create event provided", room_id)
                return (
                    0,
                    len(events_data),
                    [{"room_id": room_id, "error": "Room not found and no m.room.create event provided"}],
                    {},
                )
            
            # Extract room creator and room version from create event
            creator = create_event.get("sender")
            if not creator:
                return (
                    0,
                    len(events_data),
                    [{"room_id": room_id, "error": "m.room.create event missing sender"}],
                    {},
                )
            
            # Determine room version from create event content or use default
            create_content = create_event.get("content", {})
            room_version_str = create_content.get("room_version", "1")
            logger.info("Found room_version_str=%s for room %s", room_version_str, room_id)
            try:
                # Use getattr to access RoomVersions attributes
                room_version = getattr(RoomVersions, f"V{room_version_str}")
                logger.info("Successfully got room_version=%s for room %s", room_version.identifier, room_id)
            except AttributeError:
                # Fall back to a reasonable default if version is unrecognized
                room_version = RoomVersions.V10
                logger.warning(
                    "Unknown room version %s in create event for room %s, using %s",
                    room_version_str, room_id, room_version.identifier
                )
            
            # Create the room in the database
            try:
                await self._store.store_room(
                    room_id=room_id,
                    room_creator_user_id=creator,
                    is_public=False,  # Default to private, will be updated by state events
                    room_version=room_version,
                )
                logger.info(
                    "Auto-created room %s with version %s for creator %s",
                    room_id, room_version.identifier, creator
                )
            except Exception as store_e:
                logger.error("Failed to create room %s: %s", room_id, store_e)
                return (
                    0,
                    len(events_data),
                    [{"room_id": room_id, "error": f"Failed to create room: {str(store_e)}"}],
                    {},
                )

        # Convert event dicts to EventBase objects
        reconstructed_events = []
        failed_events = []
        errors = []
        event_id_mapping = {}  # original_event_id -> computed_event_id
        
        for event_dict in events_data:
            try:
                # Validate required fields
                required_fields = [
                    "event_id",
                    "type",
                    "sender",
                    "content",
                    "origin_server_ts",
                    "room_id",
                ]
                missing_fields = [
                    field for field in required_fields if field not in event_dict
                ]
                if missing_fields:
                    errors.append(
                        {
                            "event_id": event_dict.get("event_id", "unknown"),
                            "error": f"Missing required fields: {missing_fields}",
                        }
                    )
                    failed_events.append(event_dict)
                    continue

                # Ensure auth_events and prev_events are lists
                if "auth_events" in event_dict and not isinstance(
                    event_dict["auth_events"], list
                ):
                    event_dict["auth_events"] = []
                if "prev_events" in event_dict and not isinstance(
                    event_dict["prev_events"], list
                ):
                    event_dict["prev_events"] = []
                    
                # For room v3+, auth_events and prev_events might be provided as tuples
                # Convert them to simple lists for processing
                if event_dict.get("auth_events") and len(event_dict["auth_events"]) > 0:
                    if isinstance(event_dict["auth_events"][0], (list, tuple)):
                        logger.debug(f"Converting auth_events from tuples to list for event {event_dict.get('event_id')}")
                        event_dict["auth_events"] = [auth[0] if isinstance(auth, (list, tuple)) else auth 
                                                     for auth in event_dict["auth_events"]]
                
                if event_dict.get("prev_events") and len(event_dict["prev_events"]) > 0:
                    if isinstance(event_dict["prev_events"][0], (list, tuple)):
                        logger.debug(f"Converting prev_events from tuples to list for event {event_dict.get('event_id')}")
                        event_dict["prev_events"] = [prev[0] if isinstance(prev, (list, tuple)) else prev 
                                                     for prev in event_dict["prev_events"]]
                    
                # Auto-populate auth_events if not provided (for federation recovery)
                if not event_dict.get("auth_events"):
                    logger.debug(f"Auto-populating auth_events for event {event_dict.get('event_id')}")
                    try:
                        event_dict["auth_events"] = await self._get_required_auth_event_ids(
                            room_id,
                            event_dict["type"],
                            event_dict.get("state_key"),
                            event_dict["sender"]
                        )
                        logger.debug(f"Auto-populated auth_events: {event_dict['auth_events']}")
                    except Exception as e:
                        logger.warning(f"Failed to auto-populate auth_events: {e}")
                        event_dict["auth_events"] = []
                
                # Auto-populate prev_events if not provided (for federation recovery)
                if not event_dict.get("prev_events"):
                    logger.debug(f"Auto-populating prev_events for event {event_dict.get('event_id')}")
                    try:
                        prev_event_ids = await self._get_prev_event_ids_for_room(room_id)
                        if prev_event_ids:
                            # For room v3+, prev_events should be formatted as tuples
                            if room_version.event_format >= 3:
                                event_dict["prev_events"] = [(event_id, {}) for event_id in prev_event_ids]
                            else:
                                event_dict["prev_events"] = prev_event_ids
                            logger.debug(f"Auto-populated prev_events: {event_dict['prev_events']}")
                        else:
                            logger.debug("No forward extremities found, using empty prev_events")
                            event_dict["prev_events"] = []
                    except Exception as e:
                        logger.warning(f"Failed to auto-populate prev_events: {e}")
                        event_dict["prev_events"] = []

                # Auto-calculate depth if not provided, based on prev_events
                if "depth" not in event_dict:
                    prev_event_ids = event_dict.get("prev_events", [])
                    # Handle both list of IDs and list of tuples (room v3+)
                    if prev_event_ids and isinstance(prev_event_ids[0], tuple):
                        prev_event_ids = [event_id for event_id, _ in prev_event_ids]
                    
                    if prev_event_ids:
                        try:
                            # Get max depth of prev events and add 1
                            logger.debug(f"Getting max depth for prev_event_ids: {prev_event_ids} (type: {type(prev_event_ids)})")
                            max_depth = await self._store.get_max_depth_of(prev_event_ids)
                            event_dict["depth"] = max_depth[1] + 1 if max_depth[1] is not None else 1
                            logger.debug(f"Auto-calculated depth: {event_dict['depth']} based on prev_events")
                        except Exception as e:
                            logger.warning(f"Failed to calculate depth from prev_events: {type(e).__name__}: {e}")
                            logger.warning(f"prev_event_ids was: {prev_event_ids}")
                            event_dict["depth"] = 1
                    else:
                        # No prev events, start with depth 1
                        event_dict["depth"] = 1
                        logger.debug("No prev_events, using default depth 1")

                # Create EventBase object
                provided_event_id = event_dict.get("event_id")
                
                # For room versions 1 and 2, event_id should remain in the event dict
                # For modern room versions (3+), event_id is computed from content hash
                if room_version.event_format >= 3:  # Room v3+
                    event_dict.pop("event_id", None)  # Remove provided event_id
                
                # Debug logging before creating event
                logger.debug(f"Creating event from dict: type={event_dict.get('type')}, "
                           f"state_key={event_dict.get('state_key')}, "
                           f"room_version={room_version.identifier}")
                logger.debug(f"Event dict auth_events type: {type(event_dict.get('auth_events'))}, "
                           f"value: {event_dict.get('auth_events')}")
                logger.debug(f"Event dict prev_events type: {type(event_dict.get('prev_events'))}, "
                           f"value: {event_dict.get('prev_events')}")
                
                # Create the event with proper room version
                event = make_event_from_dict(event_dict, room_version)
                reconstructed_events.append(event)
                
                # Track the mapping for response
                event_id_mapping[provided_event_id] = event.event_id
                
                logger.debug(f"Successfully created event: {event.event_id} "
                           f"(original: {provided_event_id})")

            except Exception as e:
                logger.exception(
                    "Failed to reconstruct event %s",
                    event_dict.get("event_id", "unknown"),
                )
                errors.append(
                    {
                        "event_id": event_dict.get("event_id", "unknown"),
                        "error": f"Event reconstruction failed: {str(e)}",
                    }
                )
                failed_events.append(event_dict)
                print(f"DEBUG: Failed to reconstruct event {event_dict.get('event_id', 'unknown')}: {e}")

        if not reconstructed_events:
            return 0, len(events_data), errors, event_id_mapping

        # Sort events by depth to ensure proper processing order
        reconstructed_events.sort(key=lambda e: (e.depth, e.origin_server_ts))

        # For bulk injection, we need to ensure all auth events exist locally
        # to prevent auth validation failures when using the federation handler
        await self._ensure_auth_events_available(reconstructed_events)

        # For disaster recovery, persist events directly with appropriate context
        try:
            logger.info(
                "Starting disaster recovery persistence for %d events with mark_as_backfilled=%s",
                len(reconstructed_events), mark_as_backfilled
            )
            
            # Use our custom disaster recovery persistence that handles auth gracefully
            await self._persist_events_for_disaster_recovery(reconstructed_events, mark_as_backfilled)

            injected_count = len(reconstructed_events)
            failed_count = len(failed_events)

            logger.info(
                "Successfully persisted %d events for disaster recovery in room %s",
                injected_count,
                room_id,
            )

            # For all bulk injections, ensure room state is properly set up
            # This is critical for rooms to remain functional after injection
            print(f"DEBUG BulkEventInjection: Event types injected: {[e.type for e in reconstructed_events]}")
            print(f"DEBUG BulkEventInjection: Ensuring room state is properly set up for room {room_id}")
            await self._fix_room_state_after_bulk_injection(room_id)
            
            return injected_count, failed_count, errors, event_id_mapping

        except Exception as e:
            logger.exception("Failed to persist events for disaster recovery")
            import traceback
            error_msg = f"Event persistence failed: {type(e).__name__}: {str(e)}\nTraceback: {traceback.format_exc()}"
            print(f"DEBUG: Full error: {error_msg}")
            # If persistence fails, all events failed
            for event in reconstructed_events:
                errors.append(
                    {
                        "event_id": event.event_id,
                        "error": f"Event persistence failed: {type(e).__name__}: {str(e)}",
                    }
                )
            return 0, len(events_data), errors, event_id_mapping

    async def _persist_events_for_disaster_recovery(
        self, events: List[EventBase], mark_as_backfilled: bool
    ) -> None:
        """Persist events for disaster recovery with direct database insertion.
        
        For disaster recovery of entire rooms, we need to bypass normal event
        validation since we're restoring a complete room state from backup.
        This directly inserts events into the database with proper state tracking.
        
        Args:
            events: List of events to persist
            mark_as_backfilled: Whether to mark events as backfilled (negative stream ordering)
        """
        import json
        
        logger.info("Persisting %d events for disaster recovery", len(events))
        
        if not events:
            return
            
        room_id = events[0].room_id
        
        # For complete room recovery, we need to insert events directly
        # This bypasses auth validation which would fail for orphaned events
        print(f"DEBUG _persist_events: Using direct insertion for {len(events)} events")
        
        # Get the next stream ordering
        def _insert_events_txn(txn):
            # Get next stream ordering
            stream_gen = self._store._stream_id_gen
            stream_orderings = []
            event_stream_orderings = {}  # Map event_id to stream_ordering
            
            for event in events:
                stream_ordering = stream_gen.get_next_txn(txn)
                stream_orderings.append(stream_ordering)
                event_stream_orderings[event.event_id] = stream_ordering
                
                # Insert into events table
                self._store.db_pool.simple_insert_txn(
                    txn,
                    table="events",
                    values={
                        "event_id": event.event_id,
                        "room_id": event.room_id,
                        "type": event.type,
                        "sender": event.sender,
                        "state_key": event.state_key if hasattr(event, 'state_key') else None,
                        "depth": event.depth,
                        "stream_ordering": stream_ordering,
                        "topological_ordering": event.depth,  # Use depth as topological ordering
                        "origin_server_ts": event.origin_server_ts,
                        "received_ts": event.origin_server_ts,
                        "outlier": False,  # Not an outlier
                        "processed": True,
                        "instance_name": "master",
                    },
                )
                
                # Insert into event_json table
                self._store.db_pool.simple_insert_txn(
                    txn,
                    table="event_json",
                    values={
                        "event_id": event.event_id,
                        "room_id": event.room_id,
                        "internal_metadata": "{}",
                        "json": json.dumps(event.get_dict()),
                        "format_version": event.room_version.event_format,
                    },
                )
                
                # Insert auth events
                auth_event_ids = event.auth_event_ids()
                logger.debug(f"Inserting auth events for {event.event_id}: {auth_event_ids}")
                for auth_id in auth_event_ids:
                    # Ensure auth_id is a string, not a tuple
                    if isinstance(auth_id, tuple):
                        logger.warning(f"Auth event ID is a tuple: {auth_id}, extracting first element")
                        auth_id = auth_id[0]
                    
                    self._store.db_pool.simple_insert_txn(
                        txn,
                        table="event_auth",
                        values={
                            "event_id": event.event_id,
                            "auth_id": auth_id,
                            "room_id": event.room_id,
                        },
                    )
                
                # Insert prev events
                prev_event_ids = event.prev_event_ids()
                logger.debug(f"Inserting prev events for {event.event_id}: {prev_event_ids}")
                for prev_id in prev_event_ids:
                    # Ensure prev_id is a string, not a tuple
                    if isinstance(prev_id, tuple):
                        logger.warning(f"Prev event ID is a tuple: {prev_id}, extracting first element")
                        prev_id = prev_id[0]
                    
                    self._store.db_pool.simple_insert_txn(
                        txn,
                        table="event_edges",
                        values={
                            "event_id": event.event_id,
                            "prev_event_id": prev_id,
                            "room_id": event.room_id,
                        },
                    )
                
                # Handle state events
                if hasattr(event, 'state_key'):
                    # This is a state event
                    self._store.db_pool.simple_insert_txn(
                        txn,
                        table="state_events",
                        values={
                            "event_id": event.event_id,
                            "room_id": event.room_id,
                            "type": event.type,
                            "state_key": event.state_key,
                        },
                    )
                    
                    # Insert into current_state_events
                    self._store.db_pool.simple_upsert_txn(
                        txn,
                        table="current_state_events",
                        keyvalues={
                            "room_id": event.room_id,
                            "type": event.type,
                            "state_key": event.state_key,
                        },
                        values={
                            "event_id": event.event_id,
                            "membership": event.content.get("membership") if event.type == EventTypes.Member else None,
                            "event_stream_ordering": stream_ordering,
                        },
                    )
                    
                    # Handle membership events for local users
                    if event.type == EventTypes.Member and self._hs.is_mine_id(event.state_key):
                        membership = event.content.get("membership")
                        if membership:
                            self._store.db_pool.simple_upsert_txn(
                                txn,
                                table="local_current_membership",
                                keyvalues={
                                    "room_id": event.room_id,
                                    "user_id": event.state_key,
                                },
                                values={
                                    "event_id": event.event_id,
                                    "membership": membership,
                                },
                            )
                            
                            # Also insert into room_memberships
                            self._store.db_pool.simple_insert_txn(
                                txn,
                                table="room_memberships",
                                values={
                                    "event_id": event.event_id,
                                    "user_id": event.state_key,
                                    "sender": event.sender,
                                    "room_id": event.room_id,
                                    "membership": membership,
                                    "event_stream_ordering": stream_ordering,
                                },
                            )
                
                print(f"DEBUG _persist_events: Inserted event {event.event_id} ({event.type}) stream_ordering={stream_ordering}")
            
            # For each event, we need to create a state group
            # This is required for sync to work properly
            # Get the next state group ID by finding the max and adding 1
            txn.execute("SELECT COALESCE(MAX(id), 0) FROM state_groups")
            max_state_group = txn.fetchone()[0]
            state_group_id = max_state_group + 1
            
            # Insert state group for the room
            self._store.db_pool.simple_insert_txn(
                txn,
                table="state_groups",
                values={
                    "id": state_group_id,
                    "room_id": room_id,
                    "event_id": events[-1].event_id,  # Last event in batch
                },
            )
            
            # Map each event to the state group
            for event in events:
                self._store.db_pool.simple_insert_txn(
                    txn,
                    table="event_to_state_groups",
                    values={
                        "event_id": event.event_id,
                        "state_group": state_group_id,
                    },
                )
            
            # Update forward extremities
            if events:
                # Clear existing forward extremities
                txn.execute(
                    "DELETE FROM event_forward_extremities WHERE room_id = ?",
                    (room_id,)
                )
                
                # Set the last event as forward extremity
                last_event = events[-1]
                self._store.db_pool.simple_insert_txn(
                    txn,
                    table="event_forward_extremities",
                    values={
                        "event_id": last_event.event_id,
                        "room_id": room_id,
                    },
                )
            
            return event_stream_orderings
        
        try:
            event_stream_orderings = await self._store.db_pool.runInteraction(
                "disaster_recovery_insert_events",
                _insert_events_txn
            )
        except Exception as e:
            logger.error(f"Database insertion failed: {type(e).__name__}: {e}")
            # Check if it's the tuple error
            if "type 'tuple' is not supported" in str(e):
                logger.error("SQL tuple error detected - likely caused by tuples in event IDs")
                logger.error("This usually happens when room v3+ tuple format leaks into SQL parameters")
            raise
        
        # Clear caches - these might not have invalidate_all() method
        # TODO: Find proper way to invalidate these caches
        # self._store.get_rooms_for_user.invalidate_all()
        # self._store.get_rooms_for_local_user_where_membership_is.invalidate_all()
        
        print(f"DEBUG _persist_events: Direct insertion complete for {len(events)} events")
        
        # Notify about new events so they appear in /sync
        # This is critical for events to appear in client APIs
        if event_stream_orderings:
            notifier = self._hs.get_notifier()
            
            # Create proper stream tokens for notification
            from synapse.types import PersistedEventPosition, RoomStreamToken
            
            events_and_pos = []
            max_stream_ordering = 0
            
            for event in events:
                # Get the stream ordering we stored
                stream_ordering = event_stream_orderings.get(event.event_id)
                if stream_ordering:
                    pos = PersistedEventPosition("master", stream_ordering)
                    events_and_pos.append((event, pos))
                    max_stream_ordering = max(max_stream_ordering, stream_ordering)
                    
            if events_and_pos:
                # Create room stream token for the latest event
                room_stream_token = RoomStreamToken(stream=max_stream_ordering)
                await notifier.on_new_room_events(
                    events_and_pos,
                    room_stream_token,
                )

    async def _attempt_de_outliering_simple(self, events: List[EventBase]) -> None:
        """Simple de-outliering attempt for non-backfilled events.
        
        This tries to convert outlier events to regular events so they get positive
        stream ordering instead of being treated as backfilled.
        
        Args:
            events: List of events that were persisted as outliers
        """
        logger.info("Attempting simple de-outliering for %d events", len(events))
        
        for event in events:
            try:
                # Clear the outlier flag 
                event.internal_metadata.outlier = False
                
                # Try to compute proper event context
                context = await self._state_handler.compute_event_context(event)
                
                # Re-persist with proper context as regular event (not backfilled)
                await self._storage_controllers.persistence.persist_events(
                    [(event, context)], backfilled=False
                )
                
                logger.debug("Successfully de-outliered event %s", event.event_id)
                
            except Exception as e:
                # De-outliering failed, event will remain as outlier
                logger.debug(
                    "Failed to de-outlier event %s: %s",
                    event.event_id, e
                )
                continue

    async def _attempt_de_outliering(self, events: List[EventBase], mark_as_backfilled: bool) -> None:
        """Attempt to de-outlier events after they've been persisted as outliers.
        
        This tries to convert outlier events to regular events so they appear in
        the /messages API pagination. This is a best-effort operation that will
        succeed if the necessary auth chain and state are available.
        
        Args:
            events: List of events that were persisted as outliers
            mark_as_backfilled: Whether events should be marked as backfilled
        """
        logger.info("Attempting to de-outlier %d events", len(events))
        
        for event in events:
            try:
                # Clear the outlier flag
                event.internal_metadata.outlier = False
                
                # Try to compute proper event context
                # This will fail if auth events are missing, but that's okay
                context = await self._state_handler.compute_event_context(event)
                
                # Re-persist with proper context using the regular persistence path
                await self._storage_controllers.persistence.persist_events(
                    [(event, context)], backfilled=mark_as_backfilled
                )
                
                logger.debug("Successfully de-outliered event %s", event.event_id)
                
            except Exception as e:
                # De-outliering failed, but the event is still stored as outlier
                # This is acceptable for disaster recovery scenarios
                logger.debug(
                    "Failed to de-outlier event %s (will remain as outlier): %s",
                    event.event_id, e
                )
                continue

    async def _get_required_auth_event_ids(
        self, room_id: str, event_type: str, state_key: Optional[str], sender: str
    ) -> List[str]:
        """Get the minimal set of auth event IDs required for an event.
        
        This is used when recovering events from federation where we don't have
        the original auth_events list.
        
        Args:
            room_id: The room ID
            event_type: The type of event being authorized
            state_key: The state key if this is a state event
            sender: The sender of the event
            
        Returns:
            List of event IDs that should be used as auth_events
        """
        auth_event_ids = []
        
        try:
            # Get current state IDs using the storage controller
            state_ids = await self._storage_controllers.state.get_current_state_ids(room_id)
        except Exception as e:
            logger.warning(f"Failed to get current state IDs for room {room_id}: {e}")
            # Return empty list if we can't get state
            return auth_event_ids
        
        # Always need the create event
        create_event_id = state_ids.get((EventTypes.Create, ""))
        if create_event_id:
            auth_event_ids.append(create_event_id)
            
        # Always need the sender's membership
        sender_member_id = state_ids.get((EventTypes.Member, sender))
        if sender_member_id:
            auth_event_ids.append(sender_member_id)
            
        # Always need power levels
        power_levels_id = state_ids.get((EventTypes.PowerLevels, ""))
        if power_levels_id:
            auth_event_ids.append(power_levels_id)
            
        # For join rules, need the join rules event
        if event_type == EventTypes.Member and state_key != sender:
            join_rules_id = state_ids.get((EventTypes.JoinRules, ""))
            if join_rules_id:
                auth_event_ids.append(join_rules_id)
                
        # For invites, might need third party invite
        if event_type == EventTypes.Member:
            # Could add third party invite logic here if needed
            pass
            
        # For state events, might need the previous state
        if state_key is not None and (event_type, state_key) in state_ids:
            prev_state_id = state_ids.get((event_type, state_key))
            if prev_state_id and prev_state_id not in auth_event_ids:
                auth_event_ids.append(prev_state_id)
                
        return auth_event_ids
    
    async def _get_prev_event_ids_for_room(self, room_id: str) -> List[str]:
        """Get the appropriate prev_event IDs for a room.
        
        For federation recovery, we need to get the current forward extremities
        which represent the "tips" of the event DAG in the room.
        
        Args:
            room_id: The room ID
            
        Returns:
            List of event IDs to use as prev_events
        """
        try:
            # Get forward extremities - these return tuples of (event_id, state_group)
            extremities = await self._store.get_forward_extremities_for_room(room_id)
            
            if not extremities:
                logger.warning(f"No forward extremities found for room {room_id}")
                return []
            
            # Extract just the event IDs from the tuples
            prev_event_ids = [extremity[0] for extremity in extremities]
            
            logger.debug(f"Found {len(prev_event_ids)} forward extremities for room {room_id}")
            
            # For room v3+, we need to format prev_events as tuples with event_id and {}
            # But at this stage, we're just returning the event IDs
            # The formatting will be done when constructing the event dict
            return prev_event_ids
            
        except Exception as e:
            logger.warning(f"Failed to get forward extremities for room {room_id}: {e}")
            return []
    
    async def _fix_room_state_after_bulk_injection(self, room_id: str) -> None:
        """Fix room state after bulk injection completes.
        
        This ensures that all state tables are properly populated after bulk injection,
        including current_state_events, state_groups_state, and membership tables.
        This is critical for rooms to remain functional after disaster recovery.
        
        Args:
            room_id: The room ID to fix
        """
        print(f"DEBUG _fix_room_state: Called for room {room_id}")
        
        # First, ensure we have proper current state for the room
        # This is critical for rooms to remain functional
        await self._ensure_room_has_current_state(room_id)
        
        # Then fix state groups to ensure they have proper state mappings
        await self._ensure_state_groups_populated(room_id)
        
        
        def _fix_membership_tables(txn):
            # Find all member events in the room (including outliers)
            # Note: We specifically include outliers because bulk injection may store events as outliers
            txn.execute("""
                SELECT e.event_id, e.state_key, e.type, ej.json, e.stream_ordering, e.outlier
                FROM events e
                INNER JOIN event_json ej USING (event_id)
                WHERE e.room_id = ?
                AND e.type = 'm.room.member'
                ORDER BY e.stream_ordering DESC, e.outlier ASC
            """, (room_id,))
            
            rows = txn.fetchall()
            if not rows:
                print(f"DEBUG _fix_membership: No membership events found for room {room_id}")
                return
                
            print(f"DEBUG _fix_membership: Found {len(rows)} membership events in room {room_id}")
            for event_id, state_key, event_type, event_json_str, stream_ordering, is_outlier in rows[:5]:  # Show first 5
                print(f"DEBUG _fix_membership:   Event {event_id} for {state_key}, outlier={is_outlier}, stream={stream_ordering}")
            
            # Check if we already have current_state_events for this room
            txn.execute("""
                SELECT COUNT(*) FROM current_state_events
                WHERE room_id = ? AND type = 'm.room.member'
            """, (room_id,))
            existing_count = txn.fetchone()[0]
            
            if existing_count > 0:
                logger.info("Room %s already has %d membership entries in current_state_events", 
                           room_id, existing_count)
            
            # Group by state_key to get the latest event for each user
            latest_by_user = {}
            for event_id, state_key, event_type, event_json_str, stream_ordering, is_outlier in rows:
                # Skip if we already have a later event for this user
                if state_key in latest_by_user:
                    continue
                    
                import json
                event_json = json.loads(event_json_str)
                membership = event_json.get("content", {}).get("membership")
                if membership:
                    latest_by_user[state_key] = (event_id, membership, stream_ordering, is_outlier)
            
            # Update current_state_events for missing entries
            for user_id, (event_id, membership, stream_ordering, is_outlier) in latest_by_user.items():
                # Check if this user already has an entry
                txn.execute("""
                    SELECT event_id FROM current_state_events
                    WHERE room_id = ? AND type = ? AND state_key = ?
                """, (room_id, EventTypes.Member, user_id))
                
                existing = txn.fetchone()
                if not existing:
                    # Insert new entry
                    txn.execute("""
                        INSERT INTO current_state_events
                        (room_id, type, state_key, event_id, membership, event_stream_ordering)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (room_id, EventTypes.Member, user_id, event_id, membership, stream_ordering))
                    
                    logger.info("Added %s to current_state_events with membership %s", user_id, membership)
                
                # Always update local_current_membership for local users
                if self._hs.is_mine_id(user_id):
                    # First check if entry exists
                    txn.execute("""
                        SELECT event_id FROM local_current_membership
                        WHERE room_id = ? AND user_id = ?
                    """, (room_id, user_id))
                    
                    existing_local = txn.fetchone()
                    
                    if existing_local:
                        # Update existing entry
                        txn.execute("""
                            UPDATE local_current_membership
                            SET event_id = ?, membership = ?
                            WHERE room_id = ? AND user_id = ?
                        """, (event_id, membership, room_id, user_id))
                        logger.info("Updated local_current_membership for %s in room %s", user_id, room_id)
                    else:
                        # Insert new entry
                        txn.execute("""
                            INSERT INTO local_current_membership
                            (room_id, user_id, event_id, membership)
                            VALUES (?, ?, ?, ?)
                        """, (room_id, user_id, event_id, membership))
                        logger.info("Inserted local_current_membership for %s in room %s", user_id, room_id)
        
        await self._store.db_pool.runInteraction("fix_bulk_injection_membership", _fix_membership_tables)
        
    async def _ensure_room_has_current_state(self, room_id: str) -> None:
        """Ensure a room has entries in current_state_events table.
        
        This is needed for rooms created entirely via bulk injection, as they
        might not have gone through the normal state resolution process.
        """
        # Check if room has any current state
        has_state = await self._store.db_pool.simple_select_one_onecol(
            table="current_state_events",
            keyvalues={"room_id": room_id},
            retcol="COUNT(*)",
            desc="check_room_has_state",
        )
        
        if has_state > 0:
            print(f"DEBUG _ensure_room_has_current_state: Room {room_id} already has {has_state} current state events")
            return
            
        print(f"DEBUG _ensure_room_has_current_state: Room {room_id} has no current state, populating from events")
        
        # Get all state events for the room
        rows = await self._store.db_pool.simple_select_list(
            table="events",
            keyvalues={"room_id": room_id, "outlier": False},
            retcols=["event_id", "type", "state_key", "stream_ordering"],
            desc="get_room_state_events",
        )
        
        # Filter to only state events (have state_key)
        state_events = [row for row in rows if row["state_key"] is not None]
        
        if not state_events:
            logger.warning("No state events found for room %s", room_id)
            return
            
        # Group by (type, state_key) to get latest event
        latest_state = {}
        for row in state_events:
            key = (row["type"], row["state_key"])
            if key not in latest_state or row["stream_ordering"] > latest_state[key]["stream_ordering"]:
                latest_state[key] = row
        
        # Insert into current_state_events
        for (event_type, state_key), row in latest_state.items():
            event_id = row["event_id"]
            stream_ordering = row["stream_ordering"]
            
            # Get membership if this is a member event
            membership = None
            if event_type == EventTypes.Member:
                # Get the event JSON from event_json table
                event_json_row = await self._store.db_pool.simple_select_one(
                    table="event_json",
                    keyvalues={"event_id": event_id},
                    retcols=["json"],
                    desc="get_event_json_for_membership",
                    allow_none=True,
                )
                if event_json_row:
                    import json
                    event_data = json.loads(event_json_row["json"])
                    membership = event_data.get("content", {}).get("membership")
            
            await self._store.db_pool.simple_insert(
                table="current_state_events",
                values={
                    "event_id": event_id,
                    "room_id": room_id,
                    "type": event_type,
                    "state_key": state_key,
                    "membership": membership,
                    "event_stream_ordering": stream_ordering,
                },
                desc="populate_current_state",
            )
            
        logger.info("Populated %d current state events for room %s", len(latest_state), room_id)
    
    async def _ensure_state_groups_populated(self, room_id: str) -> None:
        """Ensure state_groups_state table is properly populated for a room.
        
        This is critical for event auth to work - when creating new events,
        Synapse needs to look up the current state from state groups.
        """
        # Get the latest state group for the room
        # We need to get the max state group ID since there might be multiple
        def get_latest_state_group(txn):
            txn.execute(
                "SELECT MAX(id) FROM state_groups WHERE room_id = ?",
                (room_id,)
            )
            row = txn.fetchone()
            return row[0] if row and row[0] is not None else None
            
        state_group_id = await self._store.db_pool.runInteraction(
            "get_latest_state_group",
            get_latest_state_group
        )
        
        if not state_group_id:
            logger.warning("No state group found for room %s", room_id)
            return
            
        # Check if state_groups_state is populated for this state group
        existing_state = await self._store.db_pool.simple_select_list(
            table="state_groups_state",
            keyvalues={"state_group": state_group_id},
            retcols=["type", "state_key", "event_id"],
            desc="check_state_group_state",
        )
        
        if existing_state:
            logger.info("State group %d already has %d state entries", state_group_id, len(existing_state))
            return
            
        logger.info("Populating state_groups_state for state group %d in room %s", state_group_id, room_id)
        
        # Get current state events manually since simple_select_list returns tuples
        def get_current_state(txn):
            txn.execute("""
                SELECT type, state_key, event_id
                FROM current_state_events
                WHERE room_id = ?
            """, (room_id,))
            return txn.fetchall()
            
        current_state_rows = await self._store.db_pool.runInteraction(
            "get_current_state_for_state_group",
            get_current_state
        )
        
        if not current_state_rows:
            logger.warning("No current state events found for room %s", room_id)
            return
            
        # Populate state_groups_state
        def _populate_state_groups_state(txn):
            for event_type, state_key, event_id in current_state_rows:
                self._store.db_pool.simple_insert_txn(
                    txn,
                    table="state_groups_state",
                    values={
                        "state_group": state_group_id,
                        "room_id": room_id,
                        "type": event_type,
                        "state_key": state_key,
                        "event_id": event_id,
                    },
                )
                
        await self._store.db_pool.runInteraction(
            "populate_state_groups_state",
            _populate_state_groups_state,
        )
        
        logger.info("Populated %d state entries in state_groups_state for room %s", len(current_state_rows), room_id)

    async def _ensure_auth_events_available(self, events: List[EventBase]) -> None:
        """Ensure all required auth events are available locally before processing.
        
        This prevents auth validation failures by making sure all auth events
        that the bulk injected events reference are already present in the database.
        
        Args:
            events: List of events to check auth events for
        """
        missing_auth_events = set()
        
        # Collect all auth event IDs referenced by the events
        for event in events:
            for auth_event_id in event.auth_event_ids():
                missing_auth_events.add(auth_event_id)
        
        if not missing_auth_events:
            return
            
        # Check which auth events we already have
        existing_auth_events = await self._store.get_events(missing_auth_events, allow_rejected=True)
        still_missing = missing_auth_events - existing_auth_events.keys()
        
        if not still_missing:
            logger.info("All auth events already available for bulk injection")
            return
            
        # For bulk injection, we expect that auth events are provided in the same batch
        # or already exist. If they're missing, we'll look for them in our events list
        events_by_id = {event.event_id: event for event in events}
        
        found_in_batch = set()
        for auth_event_id in still_missing:
            if auth_event_id in events_by_id:
                found_in_batch.add(auth_event_id)
        
        still_missing = still_missing - found_in_batch
        
        if still_missing:
            logger.warning(
                "Missing auth events for bulk injection: %s. "
                "These events may fail auth validation.",
                list(still_missing)
            )
            # For disaster recovery, we can either:
            # 1. Continue and let some events fail auth (current approach)
            # 2. Create minimal auth events  
            # 3. Skip events with missing auth
            # We'll continue for now, as the events will be processed as outliers

    async def _process_outliers_for_events(self, events: List) -> None:
        """After bulk injection, check if any events were stored as outliers and attempt to de-outlier them.
        
        This ensures that bulk injected events are properly accessible via the /messages API
        by triggering the outlier resolution process for events that may have been stored
        as outliers due to missing auth events during the initial processing.
        """
        if not events:
            return
            
        # Check which of our events are stored as outliers
        event_ids = [event.event_id for event in events]
        
        # Query the database to find which events are outliers
        outlier_info = await self._store.db_pool.runInteraction(
            "check_outlier_status",
            self._get_outlier_status_txn,
            event_ids,
        )
        
        outlier_events = []
        for event in events:
            if event.event_id in outlier_info and outlier_info[event.event_id]:
                outlier_events.append(event)
        
        if not outlier_events:
            logger.info("No outlier events found for bulk injection")
            return
            
        logger.info(f"Found {len(outlier_events)} outlier events from bulk injection, attempting to de-outlier them")
        
        # For each outlier event, try to re-process it with proper auth chain
        for event in outlier_events:
            try:
                # Mark the event as no longer an outlier
                event.internal_metadata.outlier = False
                
                # Compute proper event context
                context = await self._state_handler.compute_event_context(event)
                
                # Re-persist the event with proper context to trigger de-outliering
                await self._federation_event_handler.persist_events_and_notify(
                    event.room_id, [(event, context)], backfilled=False
                )
                
                logger.info(f"Successfully de-outliered event {event.event_id}")
                
            except Exception as e:
                logger.warning(f"Failed to de-outlier event {event.event_id}: {e}")
                # Continue with other events even if one fails

    def _get_outlier_status_txn(self, txn, event_ids: List[str]) -> Dict[str, bool]:
        """Get the outlier status for a list of event IDs"""
        if not event_ids:
            return {}
        
        # Query events table for outlier status
        placeholders = ",".join("?" for _ in event_ids)
        query = f"SELECT event_id, outlier FROM events WHERE event_id IN ({placeholders})"
        
        txn.execute(query, event_ids)
        result = {}
        for event_id, is_outlier in txn.fetchall():
            result[event_id] = bool(is_outlier)
            
        return result
