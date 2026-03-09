"""WhatsApp channel implementation using Node.js bridge.

Handles:
- WebSocket connection to Node.js Baileys bridge
- 6-type message routing (Client→Agent, Boss→Agent, Boss→Client, etc.)
- Client lookup/create with caching
- Inbound/outbound message logging
- History sync complete auto-import trigger
- LID→Phone resolution via bridge's enriched contacts
- Autopilot toggle
"""

import asyncio
import json
import os
import time
from collections import OrderedDict
from typing import Any

import httpx

from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import WhatsAppConfig


class WhatsAppChannel(BaseChannel):
    """
    WhatsApp channel that connects to a Node.js bridge.

    The bridge uses @whiskeysockets/baileys to handle the WhatsApp Web protocol.
    Communication between Python and Node.js is via WebSocket.
    """

    name = "whatsapp"

    def __init__(self, config: WhatsAppConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: WhatsAppConfig = config
        self._ws = None
        self._connected = False
        self._processed_message_ids: OrderedDict[str, None] = OrderedDict()
        self._boss_phone: str = ""  # Set by bridge on connect via __SELF_PHONE__
        # ── Performance: shared HTTP client & client cache ──
        self._http: httpx.AsyncClient | None = None
        self._client_cache: dict[str, dict] = {}   # phone → client dict
        self._cache_ts: float = 0.0                  # timestamp of last full refresh
        self._CACHE_TTL = 300                        # 5 minutes
        # ── LID→Phone resolution cache (from bridge) ──
        self._lid_to_phone: dict[str, str] = {}      # LID JID → phone number
        self._lid_cache_ts: float = 0.0
        self._LID_CACHE_TTL = 600                    # 10 minutes
        # ── Auto-import lock (prevent concurrent imports) ──
        self._import_lock = asyncio.Lock()
        self._last_import_ts: float = 0.0
        self._IMPORT_COOLDOWN = 60                   # seconds between imports

    async def start(self) -> None:
        """Start the WhatsApp channel by connecting to the bridge."""
        import websockets

        bridge_url = self.config.bridge_url

        logger.info("Connecting to WhatsApp bridge at {}...", bridge_url)

        self._running = True

        while self._running:
            try:
                async with websockets.connect(bridge_url) as ws:
                    self._ws = ws
                    # Send auth token if configured
                    if self.config.bridge_token:
                        await ws.send(json.dumps({"type": "auth", "token": self.config.bridge_token}))
                    self._connected = True
                    logger.info("Connected to WhatsApp bridge")

                    # Preload LID map from bridge on connect
                    asyncio.create_task(self._refresh_lid_cache())

                    # Listen for messages
                    async for message in ws:
                        try:
                            await self._handle_bridge_message(message)
                        except Exception as e:
                            logger.error("Error handling bridge message: {}", e)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                self._ws = None
                logger.warning("WhatsApp bridge connection error: {}", e)

                if self._running:
                    logger.info("Reconnecting in 5 seconds...")
                    await asyncio.sleep(5)

    async def stop(self) -> None:
        """Stop the WhatsApp channel."""
        self._running = False
        self._connected = False

        if self._ws:
            await self._ws.close()
            self._ws = None

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through WhatsApp."""
        if not self._ws or not self._connected:
            logger.warning("WhatsApp bridge not connected")
            return

        try:
            payload = {
                "type": "send",
                "to": msg.chat_id,
                "text": msg.content
            }
            await self._ws.send(json.dumps(payload, ensure_ascii=False))

            # Auto-log outbound message to DB
            recipient_id = msg.chat_id.split("@")[0] if "@" in msg.chat_id else msg.chat_id
            await self._log_outbound(recipient_id, msg.content)
        except Exception as e:
            logger.error("Error sending WhatsApp message: {}", e)

    async def _handle_bridge_message(self, raw: str) -> None:
        """Handle a message from the bridge."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON from bridge: {}", raw[:100])
            return

        msg_type = data.get("type")

        if msg_type == "message":
            content = data.get("content", "")

            # ── Special: self-phone identification from bridge ──
            if content == "__SELF_PHONE__":
                raw_sender = data.get("sender", "")
                self._boss_phone = raw_sender.split("@")[0] if "@" in raw_sender else raw_sender
                logger.info("Boss phone identified: {}", self._boss_phone)
                return

            # ── Special: history sync complete signal ──
            if content == "__HISTORY_SYNC_COMPLETE__":
                logger.info("History sync complete signal received from bridge")
                # Auto-import disabled — user prefers manual Import via UI
                # asyncio.create_task(self._trigger_auto_import())
                return

            # Incoming message from WhatsApp
            raw_pn = data.get("pn", "")
            raw_sender = data.get("sender", "")
            message_id = data.get("id", "")

            if message_id:
                if message_id in self._processed_message_ids:
                    return
                self._processed_message_ids[message_id] = None
                while len(self._processed_message_ids) > 1000:
                    self._processed_message_ids.popitem(last=False)

            # ── Smart routing: resolve sender to phone JID ──
            phone_jid = ""
            lid_jid = ""
            for candidate in [raw_pn, raw_sender]:
                if "@s.whatsapp.net" in candidate:
                    phone_jid = candidate
                elif "@lid" in candidate:
                    lid_jid = candidate

            if phone_jid:
                sender_id = phone_jid.split("@")[0]
                reply_chat_id = phone_jid
            elif lid_jid:
                # Try to resolve LID via bridge's LID map
                resolved = self._resolve_lid(lid_jid)
                if resolved:
                    sender_id = resolved
                    reply_chat_id = f"{resolved}@s.whatsapp.net"
                    logger.info("Resolved LID {} → phone {}", lid_jid, resolved)
                else:
                    sender_id = lid_jid.split("@")[0]
                    reply_chat_id = f"{sender_id}@s.whatsapp.net"
                    logger.warning("LID {} has no phone mapping, using raw value", lid_jid)
            else:
                fallback = raw_pn or raw_sender
                sender_id = fallback.split("@")[0] if "@" in fallback else fallback
                reply_chat_id = f"{sender_id}@s.whatsapp.net"

            logger.info("Sender {} → reply_to {} (phone_jid={}, lid_jid={})", raw_sender, reply_chat_id, phone_jid, lid_jid)

            # Handle voice transcription
            if content == "[Voice Message]":
                content = "[Voice Message: Transcription not available for WhatsApp yet]"

            # Extract display names
            push_name = data.get("pushName", "")
            contact_name = data.get("contactName", "")
            display_name = contact_name or push_name

            from_me = data.get("fromMe", False)

            # ═══════════════════════════════════════════════
            #  MESSAGE ROUTING (6-type architecture)
            # ═══════════════════════════════════════════════

            if from_me:
                if self._boss_phone and sender_id == self._boss_phone:
                    # ── Case B: Boss → Agent (self-chat command) ──
                    identity_tag = self._format_identity_tag(
                        is_owner=True,
                        sender_id=self._boss_phone,
                        display_name=display_name,
                    )
                    content = f"{identity_tag}: {data.get('content', '')}"
                    logger.info("Boss self-chat command: {}", content[:80])
                    await self._handle_message(
                        sender_id=sender_id,
                        chat_id=reply_chat_id,
                        content=content,
                        metadata={
                            "message_id": message_id,
                            "timestamp": data.get("timestamp"),
                            "is_group": data.get("isGroup", False),
                            "from_me": True,
                            "boss_phone": self._boss_phone,
                            "sender_type": "owner",
                        }
                    )
                else:
                    # ── Case C: Boss → Client (manual takeover) ──
                    logger.info("Boss manual reply to {}: {}", sender_id, data.get('content', '')[:80])
                    await self._pre_process_boss_message(sender_id, data.get("content", ""))
            else:
                # ── Case A: Client → Boss/Agent ──
                content_raw = data.get('content', '')

                # Pre-process: log to DB + client lookup
                client_info = await self._pre_process(sender_id, content_raw, from_me, display_name)
                client_name = client_info.get("name", "Unknown") if client_info else "Unknown"
                identity_tag = self._format_identity_tag(
                    is_owner=False,
                    sender_id=sender_id,
                    display_name=client_name,
                )
                content = f"{identity_tag}: {content_raw}"
                logger.info("Client message from {} ({}): {}", sender_id, client_name, content_raw[:80])

                # Check autopilot — if OFF, only log, don't send to agent
                autopilot = await self._check_autopilot()
                if autopilot:
                    await self._handle_message(
                        sender_id=sender_id,
                        chat_id=reply_chat_id,
                        content=content,
                        metadata={
                            "message_id": message_id,
                            "timestamp": data.get("timestamp"),
                            "is_group": data.get("isGroup", False),
                            "from_me": False,
                            "boss_phone": self._boss_phone,
                            "sender_type": "contact",
                        }
                    )
                else:
                    logger.info("Autopilot OFF — message logged but not sent to agent")

        elif msg_type == "status":
            status = data.get("status")
            logger.info("WhatsApp status: {}", status)

            if status == "connected":
                self._connected = True
            elif status == "disconnected":
                self._connected = False

        elif msg_type == "qr":
            logger.info("Scan QR code in the bridge terminal to connect WhatsApp")

        elif msg_type == "error":
            logger.error("WhatsApp bridge error: {}", data.get('error'))

    # ═══════════════════════════════════════════════════════════════
    # AUTO-IMPORT (triggered by history_sync_complete)
    # ═══════════════════════════════════════════════════════════════

    async def _trigger_auto_import(self) -> None:
        """Trigger backend auto-import when bridge signals history sync complete.

        Guarded by lock and cooldown to prevent concurrent/repeated imports.
        """
        if self._import_lock.locked():
            logger.info("Auto-import already in progress, skipping")
            return

        # Cooldown check
        now = time.monotonic()
        if now - self._last_import_ts < self._IMPORT_COOLDOWN:
            logger.info("Auto-import cooldown active ({:.0f}s remaining), skipping",
                        self._IMPORT_COOLDOWN - (now - self._last_import_ts))
            return

        async with self._import_lock:
            self._last_import_ts = time.monotonic()
            logger.info("Triggering backend auto-import...")
            try:
                http = self._get_http()
                resp = await http.post(
                    f"{self._api_base}/api/clients/auto-import",
                    timeout=60.0,  # Import can take a while
                )
                if resp.status_code == 200:
                    result = resp.json()
                    logger.info(
                        "Auto-import complete: created={}, updated={}, skipped={}, history={}",
                        result.get("contacts_created", 0),
                        result.get("contacts_updated", 0),
                        result.get("contacts_skipped", 0),
                        result.get("history_logged", 0),
                    )
                    errors = result.get("errors", [])
                    if errors:
                        for err in errors:
                            logger.warning("Auto-import error: {}", err)
                    # Refresh client cache after importing new clients
                    await self._refresh_client_cache()
                else:
                    logger.error("Auto-import failed: HTTP {} — {}", resp.status_code, resp.text[:200])
            except Exception as e:
                logger.error("Auto-import request failed: {}", e)

    # ═══════════════════════════════════════════════════════════════
    # LID → PHONE RESOLUTION
    # ═══════════════════════════════════════════════════════════════

    async def _refresh_lid_cache(self) -> None:
        """Fetch LID→Phone mappings from bridge's enriched contacts."""
        try:
            bridge_http = self._bridge_http_url
            http = self._get_http()
            resp = await http.get(f"{bridge_http}/contacts/enriched", timeout=10.0)
            if resp.status_code == 200:
                data = resp.json()
                contacts = data.get("contacts", [])
                self._lid_to_phone.clear()
                for c in contacts:
                    lid = c.get("lid", "")
                    phone = c.get("phone", "")
                    if lid and phone:
                        self._lid_to_phone[lid] = phone
                        # Also store without @lid suffix
                        lid_bare = lid.split("@")[0]
                        self._lid_to_phone[lid_bare] = phone
                self._lid_cache_ts = time.monotonic()
                logger.info("LID cache refreshed: {} mappings", len(self._lid_to_phone) // 2)
        except Exception as e:
            logger.warning("LID cache refresh failed: {}", e)

    def _resolve_lid(self, lid_jid: str) -> str | None:
        """Resolve a LID JID to a phone number. Returns phone digits or None."""
        # Try full JID
        phone = self._lid_to_phone.get(lid_jid)
        if phone:
            return phone
        # Try bare LID (without @lid suffix)
        bare = lid_jid.split("@")[0] if "@" in lid_jid else lid_jid
        return self._lid_to_phone.get(bare)

    @property
    def _bridge_http_url(self) -> str:
        """Derive bridge HTTP URL from the WebSocket URL."""
        # ws://bridge:3001 → http://bridge:3002
        # ws://localhost:3001 → http://localhost:3002
        ws_url = self.config.bridge_url
        http_url = ws_url.replace("ws://", "http://").replace("wss://", "https://")
        # Replace port: 3001 → 3002
        if ":3001" in http_url:
            http_url = http_url.replace(":3001", ":3002")
        elif http_url.endswith("/"):
            # If no explicit port, assume default bridge ports
            http_url = http_url.rstrip("/")
        return http_url

    # ── Shared HTTP client & client cache ────────────────────────────

    def _get_http(self) -> httpx.AsyncClient:
        """Get or create shared HTTP client."""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(timeout=5.0)
        return self._http

    async def _refresh_client_cache(self) -> None:
        """Refresh the full client cache from backend."""
        try:
            http = self._get_http()
            resp = await http.get(f"{self._api_base}/api/clients")
            clients = resp.json()
            self._client_cache.clear()
            for c in clients:
                wid = c.get("whatsapp_id", "")
                wid_clean = wid.split("@")[0] if "@" in wid else wid
                if wid_clean:
                    self._client_cache[wid_clean] = c
                if wid and wid != wid_clean:
                    self._client_cache[wid] = c
            self._cache_ts = time.monotonic()
            logger.debug("Client cache refreshed: {} entries", len(self._client_cache))
        except Exception as e:
            logger.warning("Client cache refresh failed: {}", e)

    async def _lookup_client(self, phone: str) -> dict | None:
        """Lookup client by phone with caching. Returns client dict or None."""
        # Refresh cache if stale
        if time.monotonic() - self._cache_ts > self._CACHE_TTL:
            await self._refresh_client_cache()

        # Try cache hit
        client = self._client_cache.get(phone)
        if client:
            return client

        # Miss — try one more refresh in case it's a new client
        await self._refresh_client_cache()
        return self._client_cache.get(phone)

    @property
    def _api_base(self) -> str:
        return os.environ.get("OLAINTEL_API_URL", "http://localhost:8000")

    async def _pre_process(self, sender_id: str, content: str, from_me: bool, display_name: str = "") -> dict | None:
        """Auto-log message + lookup/create client. Updates name if better name available. Returns client info dict or None."""
        try:
            http = self._get_http()

            # 1. Find client by WhatsApp ID (cached)
            client = await self._lookup_client(sender_id)

            # 2. Create client if not found
            if not client:
                resp = await http.post(f"{self._api_base}/api/clients", json={
                    "name": display_name or f"Unknown ({sender_id})",
                    "whatsapp_id": sender_id,
                    "language": "unknown",
                })
                if resp.status_code in (200, 201):
                    client = resp.json()
                    # Add to cache immediately
                    self._client_cache[sender_id] = client
                    logger.info("Created new client for {} (name={})", sender_id, display_name or "Unknown")

            # 3. Update name if current name is poor quality and we have a better one
            if client and display_name:
                old_name = client.get("name", "")
                name_is_poor = (
                    not old_name
                    or old_name == "Unknown"
                    or old_name.startswith("Unknown (")
                    or old_name.isdigit()
                    or old_name.startswith("+")
                )
                if name_is_poor:
                    await http.put(
                        f"{self._api_base}/api/clients/{client['id']}",
                        json={"name": display_name},
                    )
                    client["name"] = display_name
                    logger.info("Updated client name: {} → {} (id={})", old_name, display_name, client["id"])

            # 4. Log the inbound message
            if client:
                raw_text = content
                if raw_text.startswith("[CLIENT"):
                    raw_text = raw_text.split("]: ", 1)[-1] if "]: " in raw_text else raw_text

                await http.post(f"{self._api_base}/api/messages", json={
                    "client_id": client["id"],
                    "source": "whatsapp",
                    "direction": "inbound",
                    "original_text": raw_text,
                    "language_from": client.get("language", "unknown"),
                })
                logger.info("Logged inbound message from {} (client_id={})", sender_id, client["id"])

            return client
        except Exception as e:
            logger.warning("Pre-processing failed (non-fatal): {}", e)
            return None

    async def _pre_process_boss_message(self, recipient_id: str, content: str) -> None:
        """Log boss messages sent from any device (not just via agent) to the database."""
        try:
            http = self._get_http()
            client = await self._lookup_client(recipient_id)

            if client:
                await http.post(f"{self._api_base}/api/messages", json={
                    "client_id": client["id"],
                    "source": "whatsapp",
                    "direction": "outbound",
                    "original_text": content,
                    "language_from": client.get("language", "unknown"),
                })
                logger.info("Logged boss message to {} (client_id={})", recipient_id, client["id"])
            else:
                logger.debug("Boss message to {} — no matching client found, skipping log", recipient_id)
        except Exception as e:
            logger.warning("Boss message logging failed (non-fatal): {}", e)

    async def _check_autopilot(self) -> bool:
        """Check if autopilot mode is enabled. Defaults to OFF on failure."""
        try:
            http = self._get_http()
            resp = await http.get(f"{self._api_base}/api/profile")
            if resp.status_code == 200:
                profile = resp.json()
                return profile.get("autopilot", False)
        except Exception as e:
            logger.warning("Autopilot check failed (defaulting to OFF): {}: {}", type(e).__name__, e)
        return False

    async def _log_outbound(self, recipient_id: str, content: str) -> None:
        """Log outbound (agent reply) message to DB."""
        try:
            http = self._get_http()
            client = await self._lookup_client(recipient_id)

            if client:
                await http.post(f"{self._api_base}/api/messages", json={
                    "client_id": client["id"],
                    "source": "whatsapp",
                    "direction": "outbound",
                    "original_text": content,
                    "language_from": client.get("language", "unknown"),
                })
                logger.info("Logged outbound reply to {} (client_id={})", recipient_id, client["id"])
        except Exception as e:
            logger.warning("Outbound logging failed (non-fatal): {}", e)
