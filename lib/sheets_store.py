"""Google Sheets storage for spam flags.

One row per flag. The `dedup_key` column is the idempotency anchor — the poller
checks existing dedup_keys before appending so reruns or overlapping polls don't
double-count.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import gspread


HEADER = [
    "flagged_at_iso",
    "dedup_key",
    "trigger_type",
    "assigned_rep",
    "lead_name",
    "lead_email",
    "sfdc_link",
    "parent_ts",
    "reply_ts",
    "reporter_user_id",
    "channel_id",
    "raw_parent_excerpt",
]


@dataclass(frozen=True)
class SpamFlagRow:
    flagged_at_iso: str
    dedup_key: str
    trigger_type: str
    assigned_rep: str
    lead_name: Optional[str]
    lead_email: Optional[str]
    sfdc_link: Optional[str]
    parent_ts: str
    reply_ts: Optional[str]
    reporter_user_id: str
    channel_id: str
    raw_parent_excerpt: str

    def to_row(self) -> List[str]:
        d = asdict(self)
        return [str(d.get(k) or "") for k in HEADER]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def mention_dedup_key(reply_ts: str) -> str:
    return f"mention:{reply_ts}"


def reaction_dedup_key(parent_ts: str, reactor_user_id: str) -> str:
    return f"reaction:{parent_ts}:{reactor_user_id}"


class SheetsStore:
    def __init__(
        self,
        creds_dict: Dict[str, Any],
        sheet_id: str,
        worksheet_name: str = "Sheet1",
    ):
        gc = gspread.service_account_from_dict(creds_dict)
        sh = gc.open_by_key(sheet_id)
        self.ws = sh.worksheet(worksheet_name)
        self._ensure_header()

    def _ensure_header(self) -> None:
        current = self.ws.row_values(1)
        if not current:
            self.ws.append_row(HEADER, value_input_option="USER_ENTERED")
            return
        if current != HEADER:
            raise RuntimeError(
                f"Sheet header mismatch.\n  Expected: {HEADER}\n  Got:      {current}\n"
                "Update the sheet header or wipe the sheet and rerun."
            )

    def existing_dedup_keys(self) -> Set[str]:
        col_values = self.ws.col_values(HEADER.index("dedup_key") + 1)
        return set(col_values[1:])

    def append_flag(self, row: SpamFlagRow) -> None:
        self.ws.append_row(row.to_row(), value_input_option="USER_ENTERED")

    def fetch_flags_in_range(self, start_iso: str, end_iso: str) -> List[Dict[str, str]]:
        records = self.ws.get_all_records(expected_headers=HEADER)
        out: List[Dict[str, str]] = []
        for rec in records:
            ts = str(rec.get("flagged_at_iso", ""))
            if ts and start_iso <= ts < end_iso:
                out.append({k: str(v) for k, v in rec.items()})
        return out
