"""WhatsApp channel implementation using Node.js bridge."""

import asyncio
import json
import os
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
            # Incoming message from WhatsApp
            # Bridge sends two IDs but they can arrive in EITHER field:
            #   - phone number: <digits>@s.whatsapp.net  (what we need for replies)
            #   - LID:          <digits>@lid             (cannot send to this)
            raw_pn = data.get("pn", "")
            raw_sender = data.get("sender", "")
            content = data.get("content", "")
            message_id = data.get("id", "")

            if message_id:
                if message_id in self._processed_message_ids:
                    return
                self._processed_message_ids[message_id] = None
                while len(self._processed_message_ids) > 1000:
                    self._processed_message_ids.popitem(last=False)

            # ── Smart routing: detect which field is the phone number ──
            # Don't trust field names — pick whichever contains @s.whatsapp.net
            phone_jid = ""
            lid_jid = ""
            for candidate in [raw_pn, raw_sender]:
                if "@s.whatsapp.net" in candidate:
                    phone_jid = candidate
                elif "@lid" in candidate:
                    lid_jid = candidate

            # Use phone number for both sender_id and reply_chat_id
            if phone_jid:
                sender_id = phone_jid.split("@")[0]
                reply_chat_id = phone_jid
            elif lid_jid:
                # Fallback: only LID available (shouldn't happen normally)
                sender_id = lid_jid.split("@")[0]
                reply_chat_id = f"{sender_id}@s.whatsapp.net"
                logger.warning("Only LID available for sender {}, fabricating phone JID", sender_id)
            else:
                # Neither — use whatever we got
                fallback = raw_pn or raw_sender
                sender_id = fallback.split("@")[0] if "@" in fallback else fallback
                reply_chat_id = f"{sender_id}@s.whatsapp.net"
            
            logger.info("Sender {} → reply_to {} (phone_jid={}, lid_jid={})", raw_sender, reply_chat_id, phone_jid, lid_jid)

            # Handle voice transcription if it's a voice message
            if content == "[Voice Message]":
                logger.info("Voice message received from {}, but direct download from bridge is not yet supported.", sender_id)
                content = "[Voice Message: Transcription not available for WhatsApp yet]"

            # Extract display names from bridge (pushName = WA profile name, contactName = saved contact name)
            push_name = data.get("pushName", "")
            contact_name = data.get("contactName", "")
            display_name = contact_name or push_name  # prefer saved contact name

            # Detect if message is from the boss (fromMe = true)
            from_me = data.get("fromMe", False)
            
            # Add clear identity prefix so LLM never confuses boss/client
            if from_me:
                content = f"[BOSS]: {content}"
                logger.info("Boss message from {}: {}", sender_id, content[:80])
            else:
                content = f"[CLIENT from {sender_id}]: {content}"
                logger.info("Client message from {}: {}", sender_id, content[:80])

            # ── Pre-processing: auto-log + client lookup (Python, not LLM) ──
            if not from_me:
                client_info = await self._pre_process(sender_id, content, from_me, display_name)
                if client_info:
                    # Inject client context so agent knows who it's talking to
                    client_name = client_info.get("name", "Unknown")
                    content = f"[CLIENT from {sender_id} ({client_name})]: {data.get('content', '')}"
            else:
                # Boss messages: outbound replies get logged via send() → _log_outbound
                pass

            await self._handle_message(
                sender_id=sender_id,
                chat_id=reply_chat_id,  # Use phone-based JID, never LID
                content=content,
                metadata={
                    "message_id": message_id,
                    "timestamp": data.get("timestamp"),
                    "is_group": data.get("isGroup", False),
                    "from_me": from_me,
                }
            )
        
        elif msg_type == "status":
            # Connection status update
            status = data.get("status")
            logger.info("WhatsApp status: {}", status)
            
            if status == "connected":
                self._connected = True
            elif status == "disconnected":
                self._connected = False
        
        elif msg_type == "qr":
            # QR code for authentication
            logger.info("Scan QR code in the bridge terminal to connect WhatsApp")
        
        elif msg_type == "error":
            logger.error("WhatsApp bridge error: {}", data.get('error'))

    # ── Pre-processing pipeline ──────────────────────────────────────

    @property
    def _api_base(self) -> str:
        return os.environ.get("OLAINTEL_API_URL", "http://localhost:8000")

    async def _pre_process(self, sender_id: str, content: str, from_me: bool, display_name: str = "") -> dict | None:
        """Auto-log message + lookup/create client. Updates name if better name available. Returns client info dict or None."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as http:
                # 1. Find client by WhatsApp ID
                resp = await http.get(f"{self._api_base}/api/clients")
                clients = resp.json()
                client = None
                for c in clients:
                    wid = c.get("whatsapp_id", "")
                    # Normalize: strip @s.whatsapp.net suffix for comparison
                    wid_clean = wid.split("@")[0] if "@" in wid else wid
                    if wid_clean == sender_id or wid == sender_id:
                        client = c
                        break
                
                # 2. Create client if not found
                if not client:
                    resp = await http.post(f"{self._api_base}/api/clients", json={
                        "name": display_name or f"Unknown ({sender_id})",
                        "whatsapp_id": sender_id,
                        "language": "unknown",
                    })
                    if resp.status_code in (200, 201):
                        client = resp.json()
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
                    # Extract raw text (without [CLIENT from ...] prefix)
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

    async def _log_outbound(self, recipient_id: str, content: str) -> None:
        """Log outbound (agent reply) message to DB."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as http:
                # Find client by phone
                resp = await http.get(f"{self._api_base}/api/clients")
                clients = resp.json()
                client = None
                for c in clients:
                    wid = c.get("whatsapp_id", "")
                    wid_clean = wid.split("@")[0] if "@" in wid else wid
                    if wid_clean == recipient_id or wid == recipient_id:
                        client = c
                        break
                
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
