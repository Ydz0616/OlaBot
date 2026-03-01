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
            # Deprecated by whatsapp: old phone number style typically: <phone>@s.whatspp.net
            pn = data.get("pn", "")
            # New LID sytle typically:
            sender = data.get("sender", "")
            content = data.get("content", "")
            message_id = data.get("id", "")

            if message_id:
                if message_id in self._processed_message_ids:
                    return
                self._processed_message_ids[message_id] = None
                while len(self._processed_message_ids) > 1000:
                    self._processed_message_ids.popitem(last=False)

            # Extract phone number — prefer pn (real phone) over sender (LID)
            # pn = "8615653637766@s.whatsapp.net", sender = "181836131086344@lid"
            user_id = pn if pn else sender
            sender_id = user_id.split("@")[0] if "@" in user_id else user_id
            
            # Build proper chat_id for replies: always use phone@s.whatsapp.net
            # NEVER use @lid — Baileys can't send to LID addresses
            if pn:
                reply_chat_id = pn  # e.g. "8615653637766@s.whatsapp.net"
            elif "@lid" not in sender:
                reply_chat_id = sender  # already @s.whatsapp.net
            else:
                reply_chat_id = f"{sender_id}@s.whatsapp.net"  # fallback
            
            logger.info("Sender {} → reply_to {}", sender, reply_chat_id)

            # Handle voice transcription if it's a voice message
            if content == "[Voice Message]":
                logger.info("Voice message received from {}, but direct download from bridge is not yet supported.", sender_id)
                content = "[Voice Message: Transcription not available for WhatsApp yet]"

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
                client_info = await self._pre_process(sender_id, content, from_me)
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

    async def _pre_process(self, sender_id: str, content: str, from_me: bool) -> dict | None:
        """Auto-log message + lookup/create client. Returns client info dict or None."""
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
                        "name": f"Unknown ({sender_id})",
                        "whatsapp_id": sender_id,
                        "language": "unknown",
                    })
                    if resp.status_code in (200, 201):
                        client = resp.json()
                        logger.info("Created new client for {}", sender_id)
                
                # 3. Log the inbound message
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
