"""Weekly digest: read the sheet, group by rep, post to #spam-collection.

Schedule: Mondays at 09:00 (UTC by default in Prefect).
Window: the previous full week, Monday 00:00 UTC → next Monday 00:00 UTC.
"""

from __future__ import annotations

import json
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from prefect import flow, get_run_logger

from lib.sheets_store import SheetsStore
from lib.slack_client import build_client, post_message


def _required_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val


def _previous_full_week_utc(now: datetime) -> Tuple[datetime, datetime]:
    """Return (start_inclusive, end_exclusive) for the most recently completed Mon-Sun week in UTC."""
    this_monday = (now - timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    prev_monday = this_monday - timedelta(days=7)
    return prev_monday, this_monday


def _build_blocks(
    start: datetime,
    end: datetime,
    flags: List[Dict[str, str]],
    sheet_id: str,
) -> List[Dict[str, Any]]:
    per_rep = Counter(f.get("assigned_rep") or "(unknown)" for f in flags)
    end_inclusive = end - timedelta(seconds=1)

    header = (
        f"*Weekly Spam Summary — {start.date()} to {end_inclusive.date()}*\n"
        f"Total flagged: *{len(flags)}* lead(s)"
    )

    if not flags:
        body = "_No spam flags recorded this week._"
    else:
        lines = [
            f"• *{rep}* — {count} lead{'s' if count != 1 else ''}"
            for rep, count in per_rep.most_common()
        ]
        body = "\n".join(lines)

    sheet_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}"

    return [
        {"type": "section", "text": {"type": "mrkdwn", "text": header}},
        {"type": "divider"},
        {"type": "section", "text": {"type": "mrkdwn", "text": body}},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"<{sheet_url}|View full sheet>"},
            ],
        },
    ]


@flow(name="weekly-spam-summary")
def weekly_spam_summary() -> dict:
    log = get_run_logger()

    slack_token = _required_env("SLACK_BOT_TOKEN")
    report_channel = _required_env("SPAM_REPORTS_CHANNEL_ID")
    sheet_id = _required_env("SPAM_FLAGS_SHEET_ID")
    google_creds = json.loads(_required_env("GOOGLE_SHEETS_CREDENTIALS_JSON"))
    worksheet_name = os.getenv("SPAM_FLAGS_WORKSHEET_NAME", "Sheet1")

    now = datetime.now(timezone.utc)
    start, end = _previous_full_week_utc(now)
    log.info("Computing summary for window %s -> %s", start.isoformat(), end.isoformat())

    store = SheetsStore(google_creds, sheet_id, worksheet_name)
    flags = store.fetch_flags_in_range(start.isoformat(), end.isoformat())
    log.info("Found %d spam flag(s) in window", len(flags))

    client = build_client(slack_token)
    blocks = _build_blocks(start, end, flags, sheet_id)
    fallback_text = f"Weekly Spam Summary: {len(flags)} flag(s) recorded"
    post_message(client, report_channel, fallback_text, blocks=blocks)

    return {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "total_flags": len(flags),
    }


if __name__ == "__main__":
    weekly_spam_summary()
