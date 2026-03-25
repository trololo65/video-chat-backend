from __future__ import annotations

import asyncio
import logging
import os
import random
import uuid
from typing import Any

logger = logging.getLogger(__name__)

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class User(BaseModel):
    id: str
    name: str
    score: int
    rank: int
    avatar: str


class SignalEnvelope(BaseModel):
    type: str
    from_client: str = Field(alias="from")
    to: str | None = None
    call_id: str | None = Field(default=None, alias="callId")
    reason: str | None = None
    sdp: dict[str, Any] | None = None
    candidate: dict[str, Any] | None = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


USERS: list[User] = [
    User(
        id="user-1",
        name="Nickname",
        score=999,
        rank=1,
        avatar="https://www.figma.com/api/mcp/asset/6988852a-5e16-448b-9f1b-59ee3defae76",
    ),
    User(
        id="user-2",
        name="Nickname",
        score=896,
        rank=2,
        avatar="https://www.figma.com/api/mcp/asset/10630332-037b-4424-9948-810a6ef69261",
    ),
    User(
        id="user-3",
        name="Nickname",
        score=765,
        rank=3,
        avatar="https://www.figma.com/api/mcp/asset/a10bb1aa-cdaa-4253-95a7-988201cb22d4",
    ),
    User(
        id="user-4",
        name="Nickname",
        score=707,
        rank=4,
        avatar="https://www.figma.com/api/mcp/asset/de554f9e-d187-4a0d-9c19-6953723051f3",
    ),
    User(
        id="user-5",
        name="Nickname",
        score=691,
        rank=5,
        avatar="https://www.figma.com/api/mcp/asset/0f0bc995-051f-4500-96ae-6f7df1326088",
    ),
    User(
        id="user-6",
        name="Nickname",
        score=679,
        rank=6,
        avatar="https://www.figma.com/api/mcp/asset/45ef3596-2426-4bd2-ae73-0edff339f538",
    ),
    User(
        id="user-7",
        name="Nickname",
        score=656,
        rank=7,
        avatar="https://www.figma.com/api/mcp/asset/310ead66-49d8-4b97-9db1-a74b061f80ce",
    ),
]


def get_allowed_origins() -> list[str]:
    raw_origins = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173")
    parsed = [value.strip() for value in raw_origins.split(",") if value.strip()]
    return parsed or ["http://localhost:5173"]


class ConnectionManager:
    def __init__(self) -> None:
        self.active_connections: dict[str, WebSocket] = {}

    async def connect(self, client_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str) -> None:
        self.active_connections.pop(client_id, None)

    async def send_to(self, client_id: str, message: dict[str, Any]) -> bool:
        websocket = self.active_connections.get(client_id)
        if websocket is None:
            return False
        try:
            await websocket.send_json(message)
        except Exception:
            self.active_connections.pop(client_id, None)
            return False
        return True

    async def broadcast(
        self, message: dict[str, Any], exclude: set[str] | None = None
    ) -> None:
        excluded = exclude or set()
        for client_id in list(self.active_connections):
            if client_id in excluded:
                continue
            await self.send_to(client_id, message)


class RandomMatchManager:
    def __init__(self) -> None:
        self.connections: dict[str, WebSocket] = {}
        self.waiting_queue: list[str] = []
        self.partner_by_client: dict[str, str] = {}
        self.mode_by_client: dict[str, str] = {}
        self.lock = asyncio.Lock()

    def _cleanup_waiting_queue(self) -> None:
        self.waiting_queue = [
            client_id
            for client_id in self.waiting_queue
            if client_id in self.connections and client_id not in self.partner_by_client
        ]

    def _remove_from_waiting_queue(self, client_id: str) -> None:
        self.waiting_queue = [value for value in self.waiting_queue if value != client_id]

    async def _safe_send(self, websocket: WebSocket, payload: dict[str, Any]) -> bool:
        try:
            await websocket.send_json(payload)
        except Exception:
            return False
        return True

    async def send_to(self, client_id: str, payload: dict[str, Any]) -> bool:
        async with self.lock:
            websocket = self.connections.get(client_id)
        if websocket is None:
            return False
        return await self._safe_send(websocket, payload)

    async def connect(self, client_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self.lock:
            self.connections[client_id] = websocket
            self.mode_by_client[client_id] = "video"

    async def disconnect(self, client_id: str) -> None:
        partner_id: str | None = None

        async with self.lock:
            self.connections.pop(client_id, None)
            self._remove_from_waiting_queue(client_id)
            self.mode_by_client.pop(client_id, None)
            partner_id = self.partner_by_client.pop(client_id, None)
            if partner_id:
                self.partner_by_client.pop(partner_id, None)

        if partner_id:
            await self.send_to(
                partner_id,
                {"type": "peer_left", "from": client_id},
            )
            partner_mode = self.mode_by_client.get(partner_id, "video")
            await self.mark_ready(partner_id, "Собеседник вышел. Ищем нового...", partner_mode)

    async def leave_call(self, client_id: str) -> None:
        partner_id: str | None = None

        async with self.lock:
            self._remove_from_waiting_queue(client_id)
            partner_id = self.partner_by_client.pop(client_id, None)
            if partner_id:
                self.partner_by_client.pop(partner_id, None)

        if partner_id:
            await self.send_to(partner_id, {"type": "peer_left", "from": client_id})
            partner_mode = self.mode_by_client.get(partner_id, "video")
            await self.mark_ready(
                partner_id,
                "Собеседник завершил звонок. Ищем нового...",
                partner_mode,
            )

    async def mark_ready(
        self,
        client_id: str,
        searching_message: str | None = None,
        mode: str = "video",
    ) -> None:
        first_payload: tuple[WebSocket, dict[str, Any]] | None = None
        second_payload: tuple[WebSocket, dict[str, Any]] | None = None
        searching_ws: WebSocket | None = None
        matched_peer_id: str | None = None

        async with self.lock:
            if client_id not in self.connections:
                return

            if client_id in self.partner_by_client:
                return

            normalized_mode = "audio" if mode == "audio" else "video"
            self.mode_by_client[client_id] = normalized_mode

            if client_id not in self.waiting_queue:
                self.waiting_queue.append(client_id)

            self._cleanup_waiting_queue()

            candidates = [
                value
                for value in self.waiting_queue
                if value != client_id and self.mode_by_client.get(value, "video") == normalized_mode
            ]

            if not candidates:
                searching_ws = self.connections.get(client_id)
            else:
                partner_id = random.choice(candidates)
                self._remove_from_waiting_queue(client_id)
                self._remove_from_waiting_queue(partner_id)
                self.partner_by_client[client_id] = partner_id
                self.partner_by_client[partner_id] = client_id

                session_id = uuid.uuid4().hex
                initiator_id = random.choice([client_id, partner_id])

                client_ws = self.connections.get(client_id)
                partner_ws = self.connections.get(partner_id)
                if not client_ws or not partner_ws:
                    self.partner_by_client.pop(client_id, None)
                    self.partner_by_client.pop(partner_id, None)
                    if client_id in self.connections:
                        self.waiting_queue.append(client_id)
                    if partner_id in self.connections:
                        self.waiting_queue.append(partner_id)
                    self._cleanup_waiting_queue()
                else:
                    matched_peer_id = partner_id
                    first_payload = (
                        client_ws,
                        {
                            "type": "matched",
                            "peerId": partner_id,
                            "sessionId": session_id,
                            "initiator": initiator_id == client_id,
                            "mode": normalized_mode,
                        },
                    )
                    second_payload = (
                        partner_ws,
                        {
                            "type": "matched",
                            "peerId": client_id,
                            "sessionId": session_id,
                            "initiator": initiator_id == partner_id,
                            "mode": normalized_mode,
                        },
                    )

        if first_payload and second_payload and matched_peer_id is not None:
            ok_first = await self._safe_send(first_payload[0], first_payload[1])
            ok_second = await self._safe_send(second_payload[0], second_payload[1])
            if not (ok_first and ok_second):
                logger.warning(
                    "random_match matched delivery failed ok_first=%s ok_second=%s",
                    ok_first,
                    ok_second,
                )
                partner_id = matched_peer_id
                async with self.lock:
                    self.partner_by_client.pop(client_id, None)
                    self.partner_by_client.pop(partner_id, None)
                    for cid in (client_id, partner_id):
                        if cid in self.connections and cid not in self.waiting_queue:
                            self.waiting_queue.append(cid)
                    self._cleanup_waiting_queue()
                await self.send_to(
                    client_id,
                    {"type": "peer_left", "from": partner_id},
                )
                await self.send_to(
                    partner_id,
                    {"type": "peer_left", "from": client_id},
                )

        if searching_ws:
            await self._safe_send(
                searching_ws,
                {
                    "type": "searching",
                    "message": searching_message or "Ищем свободного собеседника...",
                },
            )

    async def relay(self, from_client_id: str, payload: dict[str, Any]) -> None:
        async with self.lock:
            explicit_target = payload.get("to")
            target_id = (
                str(explicit_target)
                if explicit_target
                else self.partner_by_client.get(from_client_id)
            )
            target_ws = self.connections.get(target_id) if target_id else None

        if not target_id or not target_ws:
            await self.send_to(
                from_client_id,
                {"type": "error", "message": "Собеседник недоступен. Ищем нового..."},
            )
            await self.leave_call(from_client_id)
            await self.mark_ready(from_client_id)
            return

        outbound = dict(payload)
        outbound.pop("to", None)
        outbound["from"] = from_client_id
        delivered = await self._safe_send(target_ws, outbound)
        if not delivered:
            logger.warning(
                "random_match relay send failed signal_type=%s",
                str(payload.get("type", "")),
            )
            await self.send_to(
                from_client_id,
                {"type": "error", "message": "Собеседник недоступен. Ищем нового..."},
            )
            await self.leave_call(from_client_id)
            await self.mark_ready(from_client_id)


direct_manager = ConnectionManager()
random_match_manager = RandomMatchManager()

app = FastAPI(title="Direct Calls API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=get_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
async def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/users", response_model=list[User])
async def get_users() -> list[User]:
    return sorted(USERS, key=lambda item: item.rank)


@app.websocket("/ws/random-match")
async def random_match_ws(websocket: WebSocket) -> None:
    client_id = uuid.uuid4().hex[:12]
    await random_match_manager.connect(client_id, websocket)
    await random_match_manager.send_to(client_id, {"type": "welcome", "clientId": client_id})

    try:
        while True:
            payload = await websocket.receive_json()
            message_type = str(payload.get("type", "")).strip()

            if message_type == "ready":
                mode = str(payload.get("mode", "video")).strip().lower()
                await random_match_manager.mark_ready(client_id, mode=mode)
                continue

            if message_type in {"webrtc_offer", "webrtc_answer", "ice_candidate"}:
                await random_match_manager.relay(client_id, payload)
                continue

            if message_type == "leave":
                await random_match_manager.leave_call(client_id)
                continue

            await random_match_manager.send_to(
                client_id,
                {"type": "error", "message": f"Unsupported message type: {message_type}"},
            )

    except WebSocketDisconnect:
        await random_match_manager.disconnect(client_id)


@app.websocket("/ws/{client_id}")
async def direct_signal_ws(websocket: WebSocket, client_id: str) -> None:
    await direct_manager.connect(client_id, websocket)

    try:
        while True:
            payload_raw = await websocket.receive_json()

            try:
                payload = SignalEnvelope.model_validate(payload_raw)
            except ValidationError as exc:
                await direct_manager.send_to(
                    client_id,
                    {
                        "type": "delivery_error",
                        "from": "server",
                        "reason": f"Invalid signaling payload: {exc.errors()[0]['msg']}",
                    },
                )
                continue

            outbound = payload.model_dump(by_alias=True, exclude_none=True)
            outbound["from"] = client_id
            target = payload.to

            if target:
                delivered = await direct_manager.send_to(target, outbound)
                if not delivered:
                    await direct_manager.send_to(
                        client_id,
                        {
                            "type": "delivery_error",
                            "from": "server",
                            "callId": payload.call_id,
                            "reason": f"Peer '{target}' is offline",
                        },
                    )
            else:
                await direct_manager.broadcast(outbound, exclude={client_id})

    except WebSocketDisconnect:
        direct_manager.disconnect(client_id)
