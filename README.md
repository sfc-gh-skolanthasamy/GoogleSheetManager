# Google Sheets Manager

A Streamlit in Snowflake app that provides bidirectional data sync between Snowflake tables and Google Sheets, with built-in scheduling via Snowflake Tasks.

## Features

- **Upload to Snowflake** -- Select a worksheet from a Google Sheet, map columns to an existing Snowflake table, and insert rows.
- **Download to Google Sheet** -- Read from a Snowflake table (with optional WHERE filter) and write to an existing or new Google Sheet/worksheet.
- **Schedule Sync** -- Create automated Snowflake Tasks with cron expressions that periodically sync a table to a worksheet.
- **Monitor** -- View all scheduled syncs and their execution history (last 7 days).

## Prerequisites

1. A Snowflake account with `ACCOUNTADMIN` (or equivalent) privileges to create the required objects.
2. A Google Cloud service account with the **Google Sheets API** and **Google Drive API** enabled.
3. The service account JSON key, stored as a Snowflake secret.

## Snowflake Setup

Create the following objects before deploying the app. Adjust names in the configuration block at the top of `streamlit_app.py` if you use different identifiers.

```sql
-- External access integration for Google APIs
CREATE OR REPLACE NETWORK RULE google_apis_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('sheets.googleapis.com', 'www.googleapis.com', 'oauth2.googleapis.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION google_sheets_access_integration
  ALLOWED_NETWORK_RULES = (google_apis_rule)
  ENABLED = TRUE;

-- Secret holding the service account key (base64-encoded or raw JSON)
CREATE OR REPLACE SECRET MY_DATA_DB.PUBLIC.google_service_account_key
  TYPE = GENERIC_STRING
  SECRET_STRING = '<your-service-account-json-or-base64>';

-- Metadata table for scheduled syncs
CREATE TABLE IF NOT EXISTS MY_DATA_DB.PUBLIC.GSHEET_SYNC_SCHEDULES (
  SCHEDULE_ID       INT AUTOINCREMENT,
  TASK_NAME         STRING,
  SOURCE_DATABASE   STRING,
  SOURCE_SCHEMA     STRING,
  SOURCE_TABLE      STRING,
  SPREADSHEET_ID    STRING,
  SPREADSHEET_NAME  STRING,
  WORKSHEET_NAME    STRING,
  CRON_EXPRESSION   STRING,
  STATUS            STRING DEFAULT 'ACTIVE',
  CREATED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

## Configuration

All environment-specific values are defined at the top of `streamlit_app.py`:

| Variable | Description | Default |
|---|---|---|
| `APP_DATABASE` | Database for procedures, tasks, and metadata | `MY_DATA_DB` |
| `APP_SCHEMA` | Schema for procedures, tasks, and metadata | `PUBLIC` |
| `APP_WAREHOUSE` | Warehouse used by scheduled tasks | `MY_WAREHOUSE` |
| `LOCAL_CONNECTION_NAME` | Snowpark connection name for local development | `SKOLANTHASAMY` |
| `SECRET_NAME` | Name of the Snowflake secret holding the Google service account key | `google_service_account_key` |
| `EAI_NAME` | External access integration name | `google_sheets_access_integration` |
| `SCHEDULE_TIMEZONE` | Timezone for cron schedules | `America/Los_Angeles` |

## Deployment

### Deploy to Snowflake (Snowsight)

1. In Snowsight, go to **Projects > Streamlit** and create a new app.
2. Upload `streamlit_app.py` and `environment.yml`.
3. Set the query warehouse and grant the app access to `google_sheets_access_integration`.

### Deploy via Snowflake CLI

```bash
snow streamlit deploy --replace
```

Requires a `snowflake.yml` in the project directory. See [Snowflake CLI docs](https://docs.snowflake.com/en/developer-guide/snowflake-cli/streamlit-apps/overview) for details.

### Run locally

```bash
pip install streamlit snowflake-snowpark-python gspread google-auth pandas
streamlit run streamlit_app.py
```

When running locally, the app falls back to the connection named in `LOCAL_CONNECTION_NAME` from `~/.snowflake/connections.toml`, and prompts you to paste service account JSON in the sidebar.

## File Structure

```
GoogleSheetManager/
├── streamlit_app.py    # Application code
├── environment.yml     # Conda dependencies (for Snowflake warehouse runtime)
├── .gitignore          # Excludes secrets, credentials, caches
└── README.md           # This file
```
