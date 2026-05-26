"""Polling flow: detect new spam flags in #sales-inbound and log them.

Triggers detected:
  1. A thread reply on a Salesforce lead notification that @mentions the bot.
  2. A `:x:` reaction added to a Salesforce lead notification (top-level message).

For each new trigger we parse the parent message for the assigned rep, append a
row to the Google Sheet, and post a confirmation reply in-thread.

Idempotency: a `dedup_key` is computed deterministically per trigger and checked
against existing sheet rows before writing. Reruns and overlapping polls are safe.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path
from typing import List

# Allow running this file directly via `python flows/poll_spam_mentions.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

from lib.salesforce_parser import (
    is_salesforce_lead_notification,
    parse_salesforce_lead,
)
from lib.sheets_store import (
    SheetsStore,
    SpamFlagRow,
    mention_dedup_key,
    reaction_dedup_key,
    utc_now_iso,
)
from lib.slack_client import (
    build_client,
    extract_message_text,
    fetch_channel_history,
    fetch_thread_replies,
    get_bot_user_id,
    message_mentions_user,
    post_thread_reply,
    reactions_on_message,
)


REACTION_EMOJI = "x"
# 7-day lookback: handles low-volume channels and recovers from polling gaps.
# Dedup by `dedup_key` makes the re-scanning safe — already-processed flags are skipped.
LOOKBACK_SECONDS = 7 * 24 * 3600
EXCERPT_CHARS = 500


def _required_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


@flow(name="poll-spam-mentions")
def poll_spam_mentions() -> dict:
    log = get_run_logger()

    slack_token = _required_env("SLACK_BOT_TOKEN")
    sales_inbound_channel = _required_env("SALES_INBOUND_CHANNEL_ID")
    sheet_id = _required_env("SPAM_FLAGS_SHEET_ID")
    google_creds = json.loads(_required_env("GOOGLE_SHEETS_CREDENTIALS_JSON"))
    worksheet_name = os.getenv("SPAM_FLAGS_WORKSHEET_NAME", "Sheet1")

    client = build_client(slack_token)
    bot_user_id = get_bot_user_id(client)
    log.info("Bot user id: %s", bot_user_id)

    store = SheetsStore(google_creds, sheet_id, worksheet_name)
    existing_keys = store.existing_dedup_keys()
    log.info("Existing dedup keys in sheet: %d", len(existing_keys))

    oldest = time.time() - LOOKBACK_SECONDS
    top_messages = fetch_channel_history(client, sales_inbound_channel, oldest=oldest)
    log.info("Fetched %d top-level messages from #sales-inbound", len(top_messages))

    new_flags: List[SpamFlagRow] = []

    for top in top_messages:
        text = extract_message_text(top)
        if not is_salesforce_lead_notification(text):
            continue
        parsed = parse_salesforce_lead(text)
        if not parsed.is_valid:
            log.warning("Lead notification at ts=%s has no Assigned To — skipping", top.get("ts"))
            continue

        parent_ts = top.get("ts")
        if not parent_ts:
            continue
        excerpt = text[:EXCERPT_CHARS]

        for reactor_id in reactions_on_message(top, REACTION_EMOJI):
            key = reaction_dedup_key(parent_ts, reactor_id)
            if key in existing_keys:
                continue
            new_flags.append(
                SpamFlagRow(
                    flagged_at_iso=utc_now_iso(),
                    dedup_key=key,
                    trigger_type="reaction",
                    assigned_rep=parsed.assigned_rep,
                    lead_name=parsed.lead_name,
                    lead_email=parsed.lead_email,
                    sfdc_link=parsed.sfdc_link,
                    parent_ts=parent_ts,
                    reply_ts=None,
                    reporter_user_id=reactor_id,
                    channel_id=sales_inbound_channel,
                    raw_parent_excerpt=excerpt,
                )
            )

        if int(top.get("reply_count") or 0) == 0:
            continue

        replies = fetch_thread_replies(client, sales_inbound_channel, parent_ts)
        for reply in replies:
            if reply.get("ts") == parent_ts:
                continue
            if reply.get("bot_id"):
                continue
            reply_text = extract_message_text(reply)
            if not message_mentions_user(reply_text, bot_user_id):
                continue
            reply_ts = reply.get("ts")
            if not reply_ts:
                continue
            key = mention_dedup_key(reply_ts)
            if key in existing_keys:
                continue
            new_flags.append(
                SpamFlagRow(
                    flagged_at_iso=utc_now_iso(),
                    dedup_key=key,
                    trigger_type="mention",
                    assigned_rep=parsed.assigned_rep,
                    lead_name=parsed.lead_name,
                    lead_email=parsed.lead_email,
                    sfdc_link=parsed.sfdc_link,
                    parent_ts=parent_ts,
                    reply_ts=reply_ts,
                    reporter_user_id=reply.get("user") or "",
                    channel_id=sales_inbound_channel,
                    raw_parent_excerpt=excerpt,
                )
            )

    log.info("Identified %d new spam flag(s) to record", len(new_flags))

    for flag in new_flags:
        store.append_flag(flag)
        existing_keys.add(flag.dedup_key)
        reporter_tag = f"<@{flag.reporter_user_id}>" if flag.reporter_user_id else "an unknown reporter"
        confirm = (
            f":white_check_mark: Logged as spam against *{flag.assigned_rep}* "
            f"(reported by {reporter_tag})"
        )
        post_thread_reply(client, flag.channel_id, flag.parent_ts, confirm)

    return {
        "new_flags": len(new_flags),
        "checked_messages": len(top_messages),
        "bot_user_id": bot_user_id,
    }


if __name__ == "__main__":
    poll_spam_mentions()
