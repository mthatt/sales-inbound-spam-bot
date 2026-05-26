"""Thin slack_sdk wrappers with retry-on-429 logic.

Reuses the same retry pattern as the existing `Inbound Slack app/export_inbound_leads.py`
to keep behavior consistent across both tools.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


_MAX_RETRIES = 5
_MAX_BACKOFF_SECONDS = 30


def _call_with_retry(fn, **kwargs) -> Dict[str, Any]:
    attempt = 0
    while True:
        try:
            return fn(**kwargs)
        except SlackApiError as e:
            status = getattr(e.response, "status_code", None)
            retry_after = None
            try:
                if e.response and e.response.headers:
                    ra = e.response.headers.get("Retry-After")
                    retry_after = int(ra) if ra is not None else None
            except Exception:
                retry_after = None

            if status == 429 and retry_after is not None:
                time.sleep(retry_after + 1)
                continue

            attempt += 1
            if attempt <= _MAX_RETRIES:
                time.sleep(min(2 ** attempt, _MAX_BACKOFF_SECONDS))
                continue
            raise


def build_client(token: str) -> WebClient:
    return WebClient(token=token)


def get_bot_user_id(client: WebClient) -> str:
    response = _call_with_retry(client.auth_test)
    return response["user_id"]


def fetch_channel_history(
    client: WebClient,
    channel_id: str,
    oldest: Optional[float] = None,
    page_limit: int = 200,
) -> List[Dict[str, Any]]:
    """Top-level messages in channel since `oldest` (unix seconds). Newest first."""
    messages: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        params: Dict[str, Any] = {"channel": channel_id, "limit": page_limit}
        if oldest is not None:
            params["oldest"] = str(oldest)
        if cursor:
            params["cursor"] = cursor
        response = _call_with_retry(client.conversations_history, **params)
        messages.extend(response.get("messages", []))
        cursor = (response.get("response_metadata") or {}).get("next_cursor") or None
        if not cursor:
            break
    return messages


def fetch_thread_replies(
    client: WebClient,
    channel_id: str,
    parent_ts: str,
    page_limit: int = 200,
) -> List[Dict[str, Any]]:
    """All replies in a thread (includes the parent as element 0)."""
    replies: List[Dict[str, Any]] = []
    cursor: Optional[str] = None
    while True:
        params: Dict[str, Any] = {
            "channel": channel_id,
            "ts": parent_ts,
            "limit": page_limit,
        }
        if cursor:
            params["cursor"] = cursor
        response = _call_with_retry(client.conversations_replies, **params)
        replies.extend(response.get("messages", []))
        cursor = (response.get("response_metadata") or {}).get("next_cursor") or None
        if not cursor:
            break
    return replies


def post_thread_reply(
    client: WebClient,
    channel_id: str,
    parent_ts: str,
    text: str,
) -> None:
    _call_with_retry(
        client.chat_postMessage,
        channel=channel_id,
        thread_ts=parent_ts,
        text=text,
    )


def post_message(
    client: WebClient,
    channel_id: str,
    text: str,
    blocks: Optional[List[Dict[str, Any]]] = None,
) -> None:
    kwargs: Dict[str, Any] = {"channel": channel_id, "text": text}
    if blocks is not None:
        kwargs["blocks"] = blocks
    _call_with_retry(client.chat_postMessage, **kwargs)


def extract_message_text(message: Dict[str, Any]) -> str:
    """Flatten text + blocks + attachments into a single string for regex parsing."""
    parts: List[str] = []
    if isinstance(message.get("text"), str):
        parts.append(message["text"])

    blocks = message.get("blocks")
    if isinstance(blocks, list):
        for block in blocks:
            if not isinstance(block, dict):
                continue
            text_obj = block.get("text")
            if isinstance(text_obj, dict) and isinstance(text_obj.get("text"), str):
                parts.append(text_obj["text"])
            fields = block.get("fields")
            if isinstance(fields, list):
                for field in fields:
                    if isinstance(field, dict) and isinstance(field.get("text"), str):
                        parts.append(field["text"])

    attachments = message.get("attachments")
    if isinstance(attachments, list):
        for att in attachments:
            if not isinstance(att, dict):
                continue
            for key in ("text", "fallback", "pretext"):
                val = att.get(key)
                if isinstance(val, str):
                    parts.append(val)

    return "\n".join(p for p in parts if p and p.strip()).strip()


def message_mentions_user(message_text: str, user_id: str) -> bool:
    """Does the message text contain a Slack mention of the given user ID?"""
    return f"<@{user_id}>" in (message_text or "")


def reactions_on_message(message: Dict[str, Any], emoji_name: str) -> List[str]:
    """Return the list of user IDs who reacted with `emoji_name` on this message."""
    for reaction in message.get("reactions") or []:
        if isinstance(reaction, dict) and reaction.get("name") == emoji_name:
            users = reaction.get("users") or []
            return [u for u in users if isinstance(u, str)]
    return []
