# Sales-Inbound Spam Bot

Two Prefect flows that turn `#sales-inbound` into a self-service spam-tagging queue:

- **`poll_spam_mentions`** (every 1 min) — watches the channel for either (a) a thread reply that @-mentions the bot, or (b) a `:x:` reaction on a Salesforce lead notification. For each, it parses the parent message for the assigned rep and appends a row to a Google Sheet. Posts a `✅ Logged as spam against <rep>` confirmation in-thread.
- **`weekly_spam_summary`** (Mondays 09:00 ET) — reads the sheet for the previous Mon–Sun, groups by rep, and posts a Block Kit digest to `#spam-collection`.

Sibling to the existing [Inbound Slack app](../Inbound%20Slack%20app/) (CSV exporter). They can share the same Slack app/token, but the spam bot needs additional scopes.

## How it works

```
Salesforce → posts lead notification in #sales-inbound
                         │
                         ▼
   ┌─ Reporter triggers: @spambot in thread, or :x: reaction ─┐
                         │
                         ▼
poll_spam_mentions (every 1m via Prefect)
   ├─ fetch top-level messages from #sales-inbound (last 24h)
   ├─ for each: parse Assigned To from the Salesforce body
   ├─ check :x: reactions  +  fetch thread replies for @-mention
   ├─ skip anything already in sheet (dedup_key)
   └─ append new flags → reply ✅ in-thread

weekly_spam_summary (Mon 09:00 ET)
   └─ read sheet → filter prior week → group by rep → post to #spam-collection
```

## Prerequisites

- A Slack workspace where you can install apps (you'll need admin or "request" rights)
- A Google Cloud project + a Google Sheet you can edit
- Prefect Cloud workspace with an ECS work pool already configured

## 1. Set up the Slack app

You can either extend the existing app used by `Inbound Slack app/` or create a new one. A new one is cleaner because it gives the bot its own display name and avatar (so confirmations are clearly attributable to the spam bot).

**Create the app** at https://api.slack.com/apps → "Create New App" → "From a manifest" → paste:

```yaml
display_information:
  name: Spam Detection Bot
  description: Logs spam/solicitation flags on inbound lead notifications.
features:
  bot_user:
    display_name: Spam Detection Bot
    always_online: true
oauth_config:
  scopes:
    bot:
      - channels:history    # read messages in #sales-inbound (if public)
      - groups:history      # read messages in #sales-inbound (if private)
      - chat:write          # post thread replies + weekly summary
      - reactions:read      # observe :x: reactions
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
```

**Install the app to your workspace** and copy the **Bot User OAuth Token** (`xoxb-...`) from "OAuth & Permissions".

**Invite the bot** to both channels:

```
/invite @spambot
```

Run this in `#sales-inbound` *and* `#spam-collection`.

## 2. Set up the Google Sheet

1. Create a new Google Sheet (or reuse one). Note the **sheet ID** from the URL: `https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit`.
2. Leave the first worksheet empty — the bot will populate the header on first run.
3. Create a Google Cloud service account: [console.cloud.google.com](https://console.cloud.google.com) → IAM & Admin → Service Accounts → "Create service account". No special roles needed.
4. On the new service account: "Keys" → "Add key" → "JSON". Download the file.
5. Open the Sheet → "Share" → paste the service account's email (`...@...iam.gserviceaccount.com`) → grant **Editor** access.

## 3. Configure Prefect

You'll need these in Prefect Cloud (or your self-hosted Prefect server):

### Secret Blocks

| Block name | Value |
|---|---|
| `slack-bot-token` | The `xoxb-...` token from step 1 |
| `google-sheets-credentials` | The **full contents** of the service-account JSON file from step 2 (as a string) |

Create via UI (`Blocks → +` → Secret) or CLI:

```bash
python -c "
from prefect.blocks.system import Secret
Secret(value='xoxb-...').save('slack-bot-token', overwrite=True)
Secret(value=open('service-account.json').read()).save('google-sheets-credentials', overwrite=True)
"
```

### Variables

| Variable name | Value | Example |
|---|---|---|
| `sales_inbound_channel_id` | Channel ID of `#sales-inbound` | `C0123456789` |
| `spam_reports_channel_id` | Channel ID of `#spam-collection` | `C0987654321` |
| `spam_flags_sheet_id` | Sheet ID from the URL | `1A2B3C...` |
| `spam_flags_worksheet_name` | Tab name in the sheet | `Sheet1` |

Set via UI (`Variables → +`) or CLI:

```bash
prefect variable set sales_inbound_channel_id C0123456789
prefect variable set spam_reports_channel_id C0987654321
prefect variable set spam_flags_sheet_id "1A2B3C..."
prefect variable set spam_flags_worksheet_name "Sheet1"
```

**To find a channel ID**: open the channel in Slack, click the name at the top → scroll to the bottom of the popup → the ID is below the channel name.

## 4. Deploy

Edit `prefect.yaml` and replace `REPLACE_WITH_YOUR_ECS_WORK_POOL` with your work pool name. Then:

```bash
cd "/Users/mihir/Documents/Code/sales-inbound-spam-bot"
prefect deploy --all
```

This deploys both flows. Verify in Prefect Cloud:

- `poll-spam-mentions / poll-spam-mentions` — schedule `*/1 * * * *` UTC
- `weekly-spam-summary / weekly-spam-summary` — schedule `0 9 * * 1` America/New_York

You can manually trigger either to test:

```bash
prefect deployment run poll-spam-mentions/poll-spam-mentions
prefect deployment run weekly-spam-summary/weekly-spam-summary
```

## 5. Try it

1. Open `#sales-inbound` and find a recent Salesforce lead post.
2. Either:
   - React to it with `:x:`, **or**
   - Reply in-thread with `@Spam Detection Bot` (Slack will autocomplete)
3. Within a minute the bot should reply: `✅ Logged as spam against <rep> (reported by <you>)`.
4. Check the Google Sheet — a new row should appear.

## Local development

```bash
cd "/Users/mihir/Documents/Code/sales-inbound-spam-bot"
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install pytest

# Run parser tests
pytest

# Run a flow locally (requires env vars set)
export SLACK_BOT_TOKEN=xoxb-...
export GOOGLE_SHEETS_CREDENTIALS_JSON="$(cat service-account.json)"
export SALES_INBOUND_CHANNEL_ID=C...
export SPAM_REPORTS_CHANNEL_ID=C...
export SPAM_FLAGS_SHEET_ID=1A2B...
python flows/poll_spam_mentions.py
```

## Project layout

```
sales-inbound-spam-bot/
├── flows/
│   ├── poll_spam_mentions.py    # the 1-min polling flow
│   └── weekly_spam_summary.py   # the Monday digest flow
├── lib/
│   ├── salesforce_parser.py     # regex extraction of Assigned To, Name, Email, SFDC Link
│   ├── slack_client.py          # slack_sdk wrappers with 429 retry
│   └── sheets_store.py          # gspread append + range reads
├── tests/
│   └── test_salesforce_parser.py
├── prefect.yaml                 # deployment config (edit work_pool name)
├── requirements.txt
└── README.md
```

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| Flow runs but `new_flags=0` despite real triggers | Bot not invited to `#sales-inbound`, or scopes missing. Check Prefect logs for Slack `not_in_channel` errors. |
| `Sheet header mismatch` error | Someone edited the sheet header. Either revert to the expected columns (see `lib/sheets_store.py` `HEADER`) or wipe the sheet. |
| Weekly summary is empty but flags exist | Window is the **prior** Mon–Sun in UTC. If today is Monday and you flagged something today, it won't appear until next Monday's digest. |
| `Lead notification at ts=X has no Assigned To` warning | Salesforce posted a message in the expected format but with an empty `Assigned To:`. Check the source record. |
| Duplicate confirmations in a thread | Shouldn't happen — dedup is by `dedup_key`. If it does, check that the sheet's `dedup_key` column wasn't accidentally cleared. |

## Cost / rate-limit notes

- Slack Web API limits are generous (Tier 3 = 50+/min for `conversations.history`). At 1-min polling on a moderate channel, you should be well below.
- Each poll = 1 `conversations.history` call + 1 `conversations.replies` call per Salesforce message with thread activity. If the channel gets hundreds of leads/day, consider raising the poll interval to 2 or 5 min.
- The Google Sheets API has a 60-write/minute-per-user limit for service accounts. We write one row per new flag plus header init — well under.
