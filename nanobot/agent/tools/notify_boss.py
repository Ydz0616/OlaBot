"""NotifyBoss tool — sends a message to the boss without LLM-fillable routing params.

This tool is the ONLY correct way for the agent to notify the boss when handling
a CONTACT message.  The boss JID is injected by the AgentLoop at the code layer
from verified metadata (boss_phone from the bridge).  The LLM cannot alter the
destination — it only supplies the message content.
"""

from typing import Any, Awaitable, Callable

from nanobot.agent.tools.base import Tool
from nanobot.bus.events import OutboundMessage


class NotifyBossTool(Tool):
    """Tool to notify the boss — destination is code-injected, not LLM-controlled."""

    def __init__(
        self,
        send_callback: Callable[[OutboundMessage], Awaitable[None]] | None = None,
        boss_channel: str = "whatsapp",
        boss_chat_id: str = "",
    ):
        self._send_callback = send_callback
        self._boss_channel = boss_channel
        self._boss_chat_id = boss_chat_id

    def set_boss(self, channel: str, chat_id: str) -> None:
        """Configure boss routing. Called by AgentLoop before each CONTACT turn."""
        self._boss_channel = channel
        self._boss_chat_id = chat_id

    def set_send_callback(self, callback: Callable[[OutboundMessage], Awaitable[None]]) -> None:
        self._send_callback = callback

    @property
    def name(self) -> str:
        return "notify_boss"

    @property
    def description(self) -> str:
        return (
            "Send a private notification to the boss (owner). "
            "Use this when handling a client message that requires the boss's attention — "
            "e.g. after creating an action, or when you can't answer a question. "
            "The destination is automatically set to the boss; do NOT use the 'message' tool for this."
        )

    @property
    def parameters(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The notification content to send to the boss. Be concise — one line preferred.",
                }
            },
            "required": ["content"],
        }

    async def execute(self, content: str, **kwargs: Any) -> str:
        if not self._boss_chat_id:
            return "Error: Boss contact not configured (boss_phone missing in metadata)"
        if not self._send_callback:
            return "Error: Message sending not configured"

        msg = OutboundMessage(
            channel=self._boss_channel,
            chat_id=self._boss_chat_id,
            content=content,
            metadata={"_notify_boss": True},
        )
        try:
            await self._send_callback(msg)
            return f"Boss notified via {self._boss_channel}"
        except Exception as e:
            return f"Error notifying boss: {e}"
