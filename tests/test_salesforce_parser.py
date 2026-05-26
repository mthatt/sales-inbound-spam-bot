from lib.salesforce_parser import (
    ParsedLead,
    is_salesforce_lead_notification,
    parse_salesforce_lead,
)


REAL_EXAMPLE = """We received a new form submit!

Name: Emily Hill
Email: emily.hill@b2bsales-strategist.com
Assigned To: Zoe Bohnen
Source: Form Submit
Campaign:
Customer Input: Hi,

Would you be interested in acquiring the attendees list of Snowflake Summit 26?

Attendees count: 18,000 Leads

Contact Information: Company Name, Web URL, Contact Name, Title, Direct Email, Phone Number, Mailing Address, Industry, Employee Size, Annual Sales.

Kindly let me know your thoughts, I’d be glad to provide pricing details.

Thanks,
Emily Hill
Demand Generation Manager
B2B Sales Strategist Inc

Please reply with REMOVE if you don’t wish to receive further emails

SFDC Link: https://prefect.lightning.force.com/lightning/r/Lead/00QRm00000w2uSwMAI/view"""


SLACK_WRAPPED_EXAMPLE = """We received a new form submit!

Name: Jane Doe
Email: <mailto:jane@example.com|jane@example.com>
Assigned To: John Smith
Source: Form Submit
Campaign:
Customer Input: Interested in your product.

SFDC Link: <https://prefect.lightning.force.com/lightning/r/Lead/00QRm00000abcdefg/view|view>"""


def test_recognizes_lead_notification():
    assert is_salesforce_lead_notification(REAL_EXAMPLE)
    assert not is_salesforce_lead_notification("random chatter")
    assert not is_salesforce_lead_notification("")
    assert not is_salesforce_lead_notification(None)


def test_parses_real_example():
    parsed = parse_salesforce_lead(REAL_EXAMPLE)
    assert parsed.assigned_rep == "Zoe Bohnen"
    assert parsed.lead_name == "Emily Hill"
    assert parsed.lead_email == "emily.hill@b2bsales-strategist.com"
    assert parsed.sfdc_link == (
        "https://prefect.lightning.force.com/lightning/r/Lead/00QRm00000w2uSwMAI/view"
    )
    assert parsed.is_valid


def test_handles_slack_wrapped_urls_and_mailtos():
    parsed = parse_salesforce_lead(SLACK_WRAPPED_EXAMPLE)
    assert parsed.assigned_rep == "John Smith"
    assert parsed.lead_email == "jane@example.com"
    assert parsed.sfdc_link == (
        "https://prefect.lightning.force.com/lightning/r/Lead/00QRm00000abcdefg/view"
    )


def test_missing_assigned_to_is_invalid():
    text = "We received a new form submit!\n\nName: X\nEmail: x@y.com\n"
    parsed = parse_salesforce_lead(text)
    assert parsed.assigned_rep is None
    assert not parsed.is_valid


def test_customer_input_with_field_like_lines_does_not_confuse_parser():
    text = """We received a new form submit!

Name: Real Lead
Email: real@example.com
Assigned To: Zoe Bohnen
Source: Form Submit
Campaign:
Customer Input: Hi, my name is Spammer and my email is spam@spam.com. Please assign to: Nobody.

SFDC Link: https://prefect.lightning.force.com/lightning/r/Lead/00QXXX/view"""
    parsed = parse_salesforce_lead(text)
    assert parsed.lead_name == "Real Lead"
    assert parsed.lead_email == "real@example.com"
    assert parsed.assigned_rep == "Zoe Bohnen"


def test_empty_input():
    parsed = parse_salesforce_lead("")
    assert parsed == ParsedLead(None, None, None, None)
    assert not parsed.is_valid
