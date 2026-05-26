"""Parse the Salesforce-for-Slack inbound-lead notification.

Expected format (key fields shown):

    We received a new form submit!

    Name: Emily Hill
    Email: emily.hill@b2bsales-strategist.com
    Assigned To: Zoe Bohnen
    Source: Form Submit
    Campaign:
    Customer Input: ...multi-line body...
    SFDC Link: https://prefect.lightning.force.com/lightning/r/Lead/00Q.../view
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional


LEAD_NOTIFICATION_MARKER = "We received a new form submit"


_NAME_RE = re.compile(r"^\s*Name:\s*(.+?)\s*$", re.MULTILINE)
_EMAIL_RE = re.compile(
    r"^\s*Email:\s*<?(?:mailto:)?([^\s<>|]+@[^\s<>|]+?)(?:\|[^>]*)?>?\s*$",
    re.MULTILINE,
)
_ASSIGNED_TO_RE = re.compile(r"^\s*Assigned To:\s*(.+?)\s*$", re.MULTILINE)
_SFDC_LINK_RE = re.compile(
    r"SFDC Link:\s*<?(https?://[^\s<>|]+)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ParsedLead:
    assigned_rep: Optional[str]
    lead_name: Optional[str]
    lead_email: Optional[str]
    sfdc_link: Optional[str]

    @property
    def is_valid(self) -> bool:
        return bool(self.assigned_rep)


def is_salesforce_lead_notification(text: Optional[str]) -> bool:
    return bool(text) and LEAD_NOTIFICATION_MARKER in text


def parse_salesforce_lead(text: Optional[str]) -> ParsedLead:
    text = text or ""

    name_m = _NAME_RE.search(text)
    email_m = _EMAIL_RE.search(text)
    assigned_m = _ASSIGNED_TO_RE.search(text)
    link_m = _SFDC_LINK_RE.search(text)

    return ParsedLead(
        assigned_rep=assigned_m.group(1).strip() if assigned_m else None,
        lead_name=name_m.group(1).strip() if name_m else None,
        lead_email=email_m.group(1).strip() if email_m else None,
        sfdc_link=link_m.group(1).strip() if link_m else None,
    )
