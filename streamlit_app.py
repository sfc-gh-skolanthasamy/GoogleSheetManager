import base64
import json
import re

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Google Sheets Manager",
    page_icon="📊",
    layout="wide",
)

# -- Configuration --
# Change these values to match your Snowflake environment.
APP_DATABASE = "MY_DATA_DB"
APP_SCHEMA = "PUBLIC"
APP_WAREHOUSE = "MY_WAREHOUSE"
LOCAL_CONNECTION_NAME = "SKOLANTHASAMY"
SECRET_NAME = "google_service_account_key"
EAI_NAME = "google_sheets_access_integration"
SCHEDULE_TIMEZONE = "America/Los_Angeles"

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# -- Session state defaults --
st.session_state.setdefault("selected_worksheet", None)
st.session_state.setdefault("gcp_creds_dict", None)

# Flag to detect Snowflake-hosted runtime
_IN_SNOWFLAKE = False
try:
    import _snowflake
    _IN_SNOWFLAKE = True
except ImportError:
    pass


def _load_creds_from_secret() -> dict | None:
    """Load Google service account credentials from the Snowflake secret."""
    if not _IN_SNOWFLAKE:
        return None
    try:
        secret_value = _snowflake.get_generic_secret_string(SECRET_NAME)
        try:
            decoded = base64.b64decode(secret_value).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            return json.loads(secret_value)
    except Exception:
        return None


# -- Google auth --
def get_gspread_client():
    """Authenticate with Google using service account credentials."""
    creds_dict = st.session_state.get("gcp_creds_dict")
    if not creds_dict:
        return None
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_snowpark_session():
    """Get a Snowpark session. Uses active session in Snowflake, falls back to local config."""
    try:
        return get_active_session()
    except Exception:
        return Session.builder.config("connection_name", LOCAL_CONNECTION_NAME).create()


# -- Data loading --


@st.cache_data(ttl=120)
def extract_spreadsheet_id(url: str) -> str | None:
    """Extract the spreadsheet ID from a Google Sheets URL or raw ID."""
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    # Allow pasting a raw spreadsheet ID (no slashes, alphanumeric + _-)
    stripped = url.strip()
    if stripped and "/" not in stripped and re.fullmatch(r"[a-zA-Z0-9_-]+", stripped):
        return stripped
    return None


@st.cache_data(ttl=120)
def get_spreadsheet_info(spreadsheet_id: str) -> dict:
    """Get spreadsheet title and worksheet names by ID."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    return {
        "name": spreadsheet.title,
        "id": spreadsheet.id,
        "worksheets": [ws.title for ws in spreadsheet.worksheets()],
    }


@st.cache_data(ttl=60)
def load_worksheet(spreadsheet_id: str, worksheet_name: str) -> pd.DataFrame:
    """Load a worksheet into a DataFrame."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    try:
        records = worksheet.get_all_records()
    except IndexError:
        # Happens when the sheet has data but no proper header row or has merged cells
        values = worksheet.get_all_values()
        if not values or len(values) < 2:
            return pd.DataFrame()
        headers = values[0]
        return pd.DataFrame(values[1:], columns=headers)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


@st.cache_data(ttl=300)
def get_table_columns(table_name: str) -> list[dict]:
    """Get column names and types for a Snowflake table."""
    session = get_snowpark_session()
    result = session.sql(f"DESCRIBE TABLE IDENTIFIER('{table_name}')").collect()
    columns = []
    for row in result:
        row_dict = row.as_dict() if hasattr(row, "as_dict") else dict(row)
        # Handle both uppercase and lowercase keys
        name = row_dict.get("name") or row_dict.get("NAME")
        col_type = row_dict.get("type") or row_dict.get("TYPE")
        if name:
            columns.append({"name": name, "type": col_type or "UNKNOWN"})
    return columns


def upload_to_snowflake(df: pd.DataFrame, table_name: str, column_mapping: dict,
                        database: str, schema: str):
    """Upload a DataFrame to an existing Snowflake table with column mapping."""
    session = get_snowpark_session()
    mapped_df = df.rename(columns=column_mapping)
    target_cols = list(column_mapping.values())
    mapped_df = mapped_df[target_cols]
    session.write_pandas(
        mapped_df,
        table_name,
        database=database,
        schema=schema,
        auto_create_table=False,
        overwrite=False,
    )


MAX_DOWNLOAD_ROWS = 10_000


def read_snowflake_table(fqn: str, limit: int = MAX_DOWNLOAD_ROWS,
                         where_clause: str = "") -> pd.DataFrame:
    """Read rows from a Snowflake table into a DataFrame (capped at `limit` rows).

    If *where_clause* is provided it is appended as a filter.  The clause is
    passed through ``DataFrame.filter()`` which safely parameterises the
    expression via Snowpark.
    """
    session = get_snowpark_session()
    df = session.table(fqn)
    if where_clause:
        df = df.filter(where_clause)
    return df.limit(limit).to_pandas()


@st.cache_data(ttl=300)
def list_databases() -> list[str]:
    """List all databases the current role can see."""
    session = get_snowpark_session()
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]


@st.cache_data(ttl=300)
def list_schemas(database: str) -> list[str]:
    """List schemas in a database."""
    session = get_snowpark_session()
    result = session.sql(f"SHOW SCHEMAS IN DATABASE IDENTIFIER('{database}')").collect()
    return [row["name"] for row in result]


@st.cache_data(ttl=300)
def list_tables_in(database: str, schema: str) -> list[str]:
    """List tables in a specific database.schema."""
    session = get_snowpark_session()
    result = session.sql(
        f"SHOW TABLES IN SCHEMA IDENTIFIER('{database}.{schema}')"
    ).collect()
    return [row["name"] for row in result]


def write_to_worksheet(spreadsheet_id: str, worksheet_name: str, df: pd.DataFrame, create_new: bool):
    """Write a DataFrame to a worksheet (existing or newly created)."""
    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    if create_new:
        worksheet = spreadsheet.add_worksheet(
            title=worksheet_name, rows=len(df) + 1, cols=len(df.columns)
        )
    else:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    if df.empty:
        return
    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    worksheet.update(range_name="A1", values=values)


def create_new_spreadsheet(title: str, worksheet_name: str, df: pd.DataFrame,
                           share_with_email: str | None = None,
                           folder_id: str | None = None) -> dict:
    """Create a brand-new Google Spreadsheet and write a DataFrame to its first sheet.

    If folder_id is provided, the spreadsheet is created inside that Google Drive
    folder (the service account must have Editor access to it).
    If share_with_email is provided, the spreadsheet is shared with that user.
    """
    client = get_gspread_client()
    spreadsheet = client.create(title, folder_id=folder_id)
    worksheet = spreadsheet.sheet1
    worksheet.update_title(worksheet_name)
    if not df.empty:
        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        worksheet.update(range_name="A1", values=values)
    if share_with_email:
        spreadsheet.share(share_with_email, perm_type="user", role="writer")
    return {"id": spreadsheet.id, "url": spreadsheet.url}


# -- Schedule helpers --

SCHEDULES_TABLE = f"{APP_DATABASE}.{APP_SCHEMA}.GSHEET_SYNC_SCHEDULES"


def _sanitize_for_proc_body(value: str) -> str:
    """Sanitize a string value for embedding inside a $$ procedure body.

    Prevents $$ escape and backslash injection by rejecting dangerous patterns.
    """
    if "$$" in value:
        raise ValueError(f"Value must not contain '$$': {value!r}")
    # Escape backslashes and double-quotes so the string stays safe inside Python quotes
    return value.replace("\\", "\\\\").replace('"', '\\"')


def create_sync_procedure(task_name: str, fqn: str, spreadsheet_id: str, worksheet_name: str):
    """Create a Python stored procedure that syncs a Snowflake table to a Google Sheet."""
    safe_task = _validate_identifier(task_name)
    # fqn is DB.SCHEMA.TABLE -- validate each part
    fqn_parts = fqn.split(".")
    if len(fqn_parts) != 3 or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in fqn_parts):
        raise ValueError(f"Invalid fully-qualified table name: {fqn!r}")
    safe_fqn = ".".join(fqn_parts)
    safe_ss_id = _sanitize_for_proc_body(spreadsheet_id)
    safe_ws_name = _sanitize_for_proc_body(worksheet_name)

    session = get_snowpark_session()
    fq_prefix = f"{APP_DATABASE}.{APP_SCHEMA}"
    fq_secret = f"{fq_prefix}.{SECRET_NAME}"
    proc_sql = f"""
CREATE OR REPLACE PROCEDURE {fq_prefix}.{safe_task}_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'gspread', 'google-auth')
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = ({EAI_NAME})
SECRETS = ('{SECRET_NAME}' = {fq_secret})
AS
$$
import base64
import json
import _snowflake
import gspread
from google.oauth2.service_account import Credentials
from snowflake.snowpark.context import get_active_session

def run(session=None):
    if session is None:
        session = get_active_session()
    secret_value = _snowflake.get_generic_secret_string("{SECRET_NAME}")
    try:
        decoded = base64.b64decode(secret_value).decode("utf-8")
        creds_dict = json.loads(decoded)
    except Exception:
        creds_dict = json.loads(secret_value)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    df = session.table("{safe_fqn}").to_pandas()
    spreadsheet = gc.open_by_key("{safe_ss_id}")
    try:
        worksheet = spreadsheet.worksheet("{safe_ws_name}")
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title="{safe_ws_name}", rows=len(df) + 1, cols=len(df.columns)
        )
    if df.empty:
        return "No data to sync"
    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    worksheet.update(range_name="A1", values=values)
    return f"Synced {{len(df)}} rows to {safe_ws_name}"
$$
"""
    session.sql(proc_sql).collect()


def create_sync_task(task_name: str, cron_expr: str):
    """Create and resume a Snowflake task that calls the sync procedure."""
    safe_task = _validate_identifier(task_name)
    # Validate cron expression: only allow digits, spaces, *, /, -, comma
    if not re.fullmatch(r"[0-9 */,\-]+", cron_expr):
        raise ValueError(f"Invalid CRON expression: {cron_expr!r}")
    session = get_snowpark_session()
    fq_prefix = f"{APP_DATABASE}.{APP_SCHEMA}"
    session.sql(f"""
CREATE OR REPLACE TASK {fq_prefix}.{safe_task}
    WAREHOUSE = {APP_WAREHOUSE}
    SCHEDULE = 'USING CRON {cron_expr} {SCHEDULE_TIMEZONE}'
AS
    CALL {fq_prefix}.{safe_task}_PROC()
""").collect()
    session.sql(f"ALTER TASK {fq_prefix}.{safe_task} RESUME").collect()


def insert_schedule_record(task_name, src_db, src_schema, src_table,
                           spreadsheet_id, spreadsheet_name, worksheet_name, cron_expr):
    """Insert a row into the schedules metadata table."""
    session = get_snowpark_session()
    session.sql(
        f"INSERT INTO {SCHEDULES_TABLE}"
        " (TASK_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,"
        "  SPREADSHEET_ID, SPREADSHEET_NAME, WORKSHEET_NAME, CRON_EXPRESSION)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        params=[task_name, src_db, src_schema, src_table,
                spreadsheet_id, spreadsheet_name, worksheet_name, cron_expr],
    ).collect()


@st.cache_data(ttl=30)
def list_schedules() -> pd.DataFrame:
    """Read all schedule records."""
    session = get_snowpark_session()
    return session.table(SCHEDULES_TABLE).to_pandas()


def _validate_identifier(name: str) -> str:
    """Validate that a string is a safe Snowflake identifier (alphanumeric + underscores)."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def delete_schedule(task_name: str, schedule_id: int):
    """Drop the task, procedure, and metadata row."""
    safe_name = _validate_identifier(task_name)
    session = get_snowpark_session()
    fq_prefix = f"{APP_DATABASE}.{APP_SCHEMA}"
    try:
        session.sql(f"ALTER TASK {fq_prefix}.{safe_name} SUSPEND").collect()
    except Exception:
        pass
    try:
        session.sql(f"DROP TASK IF EXISTS {fq_prefix}.{safe_name}").collect()
    except Exception:
        pass
    try:
        session.sql(f"DROP PROCEDURE IF EXISTS {fq_prefix}.{safe_name}_PROC()").collect()
    except Exception:
        pass
    session.sql(
        f"DELETE FROM {SCHEDULES_TABLE} WHERE SCHEDULE_ID = ?",
        params=[schedule_id],
    ).collect()


def get_task_history(task_name: str) -> pd.DataFrame:
    """Get recent execution history for a task."""
    safe_name = _validate_identifier(task_name)
    session = get_snowpark_session()
    result = session.sql(f"""
SELECT SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME, STATE, ERROR_MESSAGE, RETURN_VALUE
FROM TABLE({APP_DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => '{safe_name}',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -7, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 50
))
ORDER BY SCHEDULED_TIME DESC
""").collect()
    if not result:
        return pd.DataFrame()
    return pd.DataFrame([row.as_dict() for row in result])


# -- UI --
st.title("Google Sheets Manager")

# Sidebar: connection status and configuration
with st.sidebar:
    st.header("Snowflake")

    try:
        session = get_snowpark_session()
        user_result = session.sql("SELECT CURRENT_USER()").collect()
        current_user = user_result[0][0] if user_result else "unknown"
        st.success(f"Connected as **{current_user}**")
    except Exception as e:
        st.error(f"Snowflake connection failed: {e}")
        st.caption(
            f"If running locally, check that the {LOCAL_CONNECTION_NAME} connection is configured "
            "in ~/.snowflake/connections.toml."
        )
        st.stop()

    st.divider()
    st.header("Google Drive")

    # Try loading credentials from Snowflake secret first
    if st.session_state.get("gcp_creds_dict") is None:
        secret_creds = _load_creds_from_secret()
        if secret_creds:
            st.session_state["gcp_creds_dict"] = secret_creds

    # If not in Snowflake (no secret available), allow pasting JSON manually
    if not _IN_SNOWFLAKE and st.session_state.get("gcp_creds_dict") is None:
        json_text = st.text_area(
            "Paste service account JSON",
            height=150,
            key="sa_json_text",
        )
        if json_text:
            try:
                creds_dict = json.loads(json_text)
                st.session_state["gcp_creds_dict"] = creds_dict
            except Exception as e:
                st.error(f"Invalid JSON: {e}")

    if st.session_state.get("gcp_creds_dict") is None:
        st.info("Google credentials not available. Check that the secret is configured.")
        st.stop()

    sa_email = st.session_state["gcp_creds_dict"].get("client_email", "unknown")

    try:
        client = get_gspread_client()
        # Verify the client works by checking it's not None
        if client is None:
            raise ValueError("Failed to create gspread client")
        st.success("Connected to Google")
        st.caption(f"Service account: `{sa_email}`")
    except Exception as e:
        st.error(f"Google auth failed: {e}")
        st.stop()

    st.divider()
    sheet_url = st.text_input(
        "Google Sheet URL",
        placeholder="https://docs.google.com/spreadsheets/d/.../edit",
        key="sheet_url_input",
    )

    if not sheet_url:
        st.info(
            "Paste a Google Sheet URL above to get started. "
            f"Make sure the sheet is shared with **{sa_email}** (Editor access)."
        )
        st.stop()

    ss_id = extract_spreadsheet_id(sheet_url)
    if not ss_id:
        st.error("Could not extract a spreadsheet ID from the URL. Please check the link.")
        st.stop()

    try:
        ss_info = get_spreadsheet_info(ss_id)
    except Exception as e:
        st.error(f"Failed to open spreadsheet: {e}")
        st.caption(
            f"Make sure the sheet is shared with **{sa_email}** (Editor access)."
        )
        st.stop()

    st.caption(f"Spreadsheet: **{ss_info['name']}**")

    selected_ws = st.selectbox(
        "Worksheet",
        ss_info["worksheets"],
        key="worksheet_selector",
    )
    st.session_state.selected_worksheet = selected_ws


# Main content: tabs
ws_name = st.session_state.selected_worksheet

tab_upload, tab_download, tab_schedule, tab_monitor = st.tabs(
    ["Upload to Snowflake", "Download to Google Sheet",
     "Schedule Sync", "Monitor"]
)

# -- UPLOAD TO SNOWFLAKE TAB --
with tab_upload:
    with st.spinner("Loading sheet data..."):
        df = load_worksheet(ss_id, ws_name)

    if df.empty:
        st.info("This worksheet is empty. Nothing to upload.")
    else:
        st.subheader("Source data preview")
        st.dataframe(df.head(5), use_container_width=True)

        st.subheader("Target Snowflake table")

        upload_ok = True
        try:
            up_databases = list_databases()
        except Exception as e:
            st.error(f"Failed to list databases: {e}")
            up_databases = []
            upload_ok = False

        if upload_ok and not up_databases:
            st.warning("No databases found.")
            upload_ok = False

        if upload_ok:
            up_database = st.selectbox("Database", up_databases, key="up_database")

            try:
                up_schemas = list_schemas(up_database)
            except Exception as e:
                st.error(f"Failed to list schemas: {e}")
                up_schemas = []
                upload_ok = False

        if upload_ok and not up_schemas:
            st.warning(f"No schemas found in {up_database}.")
            upload_ok = False

        if upload_ok:
            up_schema = st.selectbox("Schema", up_schemas, key="up_schema")

            try:
                tables = list_tables_in(up_database, up_schema)
            except Exception as e:
                st.error(f"Failed to list tables: {e}")
                tables = []
                upload_ok = False

        if upload_ok and not tables:
            st.warning(
                f"No tables found in {up_database}.{up_schema}. "
                "Create a table first, then upload data to it."
            )
            upload_ok = False

        if upload_ok:
            target_table = st.selectbox("Table", tables, key="up_table")
            up_fqn = f"{up_database}.{up_schema}.{target_table}"

            try:
                table_cols = get_table_columns(up_fqn)
            except Exception as e:
                st.error(f"Failed to describe table: {e}")
                table_cols = []
            sf_col_names = [c["name"] for c in table_cols]
            sheet_col_names = df.columns.tolist()

            if not sf_col_names:
                st.warning("Could not retrieve columns for this table.")
                upload_ok = False

        if upload_ok:
            st.subheader("Column mapping")
            st.caption("Map each sheet column to a Snowflake table column.")

            column_mapping = {}
            cols = st.columns(2)
            cols[0].markdown("**Sheet column**")
            cols[1].markdown("**Snowflake column**")

            for sheet_col in sheet_col_names:
                col_left, col_right = st.columns(2)
                col_left.text(sheet_col)

                default_idx = 0
                sheet_col_upper = sheet_col.upper()
                for i, sf_col in enumerate(sf_col_names):
                    if sf_col.upper() == sheet_col_upper:
                        default_idx = i
                        break

                mapped_col = col_right.selectbox(
                    f"Map '{sheet_col}'",
                    sf_col_names,
                    index=default_idx,
                    key=f"map_{sheet_col}",
                    label_visibility="collapsed",
                )
                column_mapping[sheet_col] = mapped_col

            mapped_targets = list(column_mapping.values())
            duplicates = [c for c in set(mapped_targets) if mapped_targets.count(c) > 1]

            if duplicates:
                st.warning(
                    f"Multiple sheet columns map to the same Snowflake column: "
                    f"{', '.join(duplicates)}. Fix the mapping before uploading."
                )

            if st.button(
                "Upload to Snowflake",
                type="primary",
                disabled=len(duplicates) > 0,
            ):
                with st.spinner(f"Uploading {len(df)} rows to {up_fqn}..."):
                    try:
                        upload_to_snowflake(df, target_table, column_mapping,
                                            up_database, up_schema)
                        st.success(
                            f"Successfully uploaded {len(df)} rows to **{up_fqn}**."
                        )
                    except Exception as e:
                        st.error(f"Upload failed: {e}")


# -- DOWNLOAD TO GOOGLE SHEET TAB --
with tab_download:
    st.subheader("Source Snowflake table")

    dl_ok = True
    try:
        databases = list_databases()
    except Exception as e:
        st.error(f"Failed to list databases: {e}")
        databases = []
        dl_ok = False

    if dl_ok and not databases:
        st.warning("No databases found.")
        dl_ok = False

    if dl_ok:
        dl_database = st.selectbox("Database", databases, key="dl_database")

        try:
            schemas = list_schemas(dl_database)
        except Exception as e:
            st.error(f"Failed to list schemas: {e}")
            schemas = []
            dl_ok = False

    if dl_ok and not schemas:
        st.warning(f"No schemas found in {dl_database}.")
        dl_ok = False

    if dl_ok:
        dl_schema = st.selectbox("Schema", schemas, key="dl_schema")

        try:
            dl_tables = list_tables_in(dl_database, dl_schema)
        except Exception as e:
            st.error(f"Failed to list tables: {e}")
            dl_tables = []
            dl_ok = False

    if dl_ok and not dl_tables:
        st.warning(f"No tables found in {dl_database}.{dl_schema}.")
        dl_ok = False

    if dl_ok:
        source_table = st.selectbox("Table", dl_tables, key="dl_source_table")
        fqn = f"{dl_database}.{dl_schema}.{source_table}"

        dl_row_limit = st.number_input(
            "Max rows to download",
            min_value=1,
            max_value=MAX_DOWNLOAD_ROWS,
            value=MAX_DOWNLOAD_ROWS,
            step=1000,
            key="dl_row_limit",
            help=f"Maximum allowed: {MAX_DOWNLOAD_ROWS:,} rows",
        )

        dl_where = st.text_input(
            "WHERE clause (optional)",
            placeholder='e.g. STATUS = \'ACTIVE\' AND CREATED_AT > \'2024-01-01\'',
            key="dl_where_clause",
            help="Filter rows before downloading. Enter a SQL boolean expression (without the WHERE keyword).",
        )

        if st.button("Preview table data"):
            st.session_state["dl_preview"] = True

        if st.session_state.get("dl_preview"):
            with st.spinner(f"Reading {fqn}..."):
                try:
                    sf_df = read_snowflake_table(fqn, limit=dl_row_limit, where_clause=dl_where)
                except Exception as e:
                    st.error(f"Failed to read table: {e}")
                    sf_df = None

            if sf_df is not None:
                row_note = f" (capped at {dl_row_limit:,})" if len(sf_df) >= dl_row_limit else ""
                st.caption(f"{len(sf_df):,} rows{row_note}, {len(sf_df.columns)} columns")
                st.dataframe(sf_df.head(10), use_container_width=True)

        st.subheader("Target Google Sheet")

        dl_target = st.radio(
            "Destination",
            ["Existing spreadsheet", "New spreadsheet"],
            key="dl_target_type",
        )

        if dl_target == "New spreadsheet":
            st.caption(
                "Create a Google Drive folder, share it with the service account "
                f"(**{sa_email}**) as Editor, then paste the folder ID below. "
                "The folder ID is the last part of the folder URL: "
                "`https://drive.google.com/drive/folders/<FOLDER_ID>`"
            )
            dl_folder_id = st.text_input(
                "Google Drive folder ID",
                placeholder="e.g. 1aBcDeFgHiJkLmNoPqRsTuVwXyZ",
                key="dl_folder_id",
            )
            new_ss_title = st.text_input(
                "New spreadsheet name",
                value=source_table,
                key="dl_new_ss_title",
            )
            new_ss_ws_name = st.text_input(
                "Worksheet name",
                value="Sheet1",
                key="dl_new_ss_ws_name",
            )
            share_email = st.text_input(
                "Share with (your Google email)",
                placeholder="you@example.com",
                key="dl_share_email",
                help="Optional. The spreadsheet will also be shared directly with this email.",
            )

            if st.button("Download to new Google Sheet", type="primary"):
                if not dl_folder_id:
                    st.warning("Enter a Google Drive folder ID to create the spreadsheet in.")
                else:
                    with st.spinner(f"Reading {fqn} and creating new spreadsheet..."):
                        try:
                            sf_df = read_snowflake_table(fqn, limit=dl_row_limit, where_clause=dl_where)
                            result = create_new_spreadsheet(
                                new_ss_title, new_ss_ws_name, sf_df,
                                share_with_email=share_email or None,
                                folder_id=dl_folder_id,
                            )
                            st.success(
                                f"Created spreadsheet **{new_ss_title}** with "
                                f"{len(sf_df):,} rows in worksheet **{new_ss_ws_name}**."
                            )
                            st.caption(f"Spreadsheet URL: {result['url']}")
                        except Exception as e:
                            st.error(f"Download failed: {e}")

        else:
            st.caption(f"Spreadsheet: **{ss_info['name']}**")

            ws_option = st.radio(
                "Write to",
                ["Existing worksheet (overwrites data)", "New worksheet"],
                key="dl_ws_option",
            )

            if ws_option == "New worksheet":
                new_ws_name = st.text_input(
                    "New worksheet name",
                    value=source_table,
                    key="dl_new_ws_name",
                )
                target_ws_name = new_ws_name
                create_new = True
            else:
                target_ws_name = st.selectbox(
                    "Select worksheet",
                    ss_info["worksheets"],
                    key="dl_existing_ws",
                )
                create_new = False

            if st.button("Download to Google Sheet", type="primary"):
                with st.spinner(f"Reading {fqn} and writing to '{target_ws_name}'..."):
                    try:
                        sf_df = read_snowflake_table(fqn, limit=dl_row_limit, where_clause=dl_where)
                        write_to_worksheet(ss_id, target_ws_name, sf_df, create_new)
                        st.success(
                            f"Successfully wrote {len(sf_df):,} rows to "
                            f"worksheet **{target_ws_name}** in **{ss_info['name']}**."
                        )
                    except Exception as e:
                        st.error(f"Download failed: {e}")


# -- SCHEDULE SYNC TAB --
with tab_schedule:
    st.subheader("Create a scheduled sync")
    st.caption("Periodically export a Snowflake table to a Google Sheet worksheet.")

    sched_ok = True

    # -- Source table selection --
    st.markdown("**Source Snowflake table**")
    try:
        sched_databases = list_databases()
    except Exception as e:
        st.error(f"Failed to list databases: {e}")
        sched_databases = []
        sched_ok = False

    if sched_ok and not sched_databases:
        st.warning("No databases found.")
        sched_ok = False

    if sched_ok:
        sched_db = st.selectbox("Database", sched_databases, key="sched_db")

        try:
            sched_schemas = list_schemas(sched_db)
        except Exception as e:
            st.error(f"Failed to list schemas: {e}")
            sched_schemas = []
            sched_ok = False

    if sched_ok and not sched_schemas:
        st.warning(f"No schemas found in {sched_db}.")
        sched_ok = False

    if sched_ok:
        sched_schema = st.selectbox("Schema", sched_schemas, key="sched_schema")

        try:
            sched_tables = list_tables_in(sched_db, sched_schema)
        except Exception as e:
            st.error(f"Failed to list tables: {e}")
            sched_tables = []
            sched_ok = False

    if sched_ok and not sched_tables:
        st.warning(f"No tables found in {sched_db}.{sched_schema}.")
        sched_ok = False

    if sched_ok:
        sched_table = st.selectbox("Table", sched_tables, key="sched_table")

        # -- Target Google Sheet selection --
        st.markdown("**Target Google Sheet**")

        sched_sheet_url = st.text_input(
            "Google Sheet URL",
            value=sheet_url,
            key="sched_sheet_url",
        )
        sched_ss_id = extract_spreadsheet_id(sched_sheet_url) if sched_sheet_url else None

        if not sched_ss_id:
            st.warning("Enter a valid Google Sheet URL above.")
            sched_ok = False

        if sched_ok:
            try:
                sched_ss_info = get_spreadsheet_info(sched_ss_id)
            except Exception as e:
                st.error(f"Failed to open spreadsheet: {e}")
                sched_ok = False

    if sched_ok:
        sched_ss_name = sched_ss_info["name"]

        sched_ws_option = st.radio(
            "Worksheet",
            ["Existing worksheet", "New worksheet"],
            key="sched_ws_option",
        )
        if sched_ws_option == "New worksheet":
            sched_ws_name = st.text_input(
                "New worksheet name",
                value=sched_table,
                key="sched_new_ws",
            )
        else:
            sched_ws_name = st.selectbox(
                "Select worksheet",
                sched_ss_info["worksheets"],
                key="sched_existing_ws",
            )

        # -- Schedule frequency --
        st.markdown("**Schedule**")

        cron_presets = {
            "Every hour": "0 * * * *",
            "Every 6 hours": "0 */6 * * *",
            "Every 12 hours": "0 */12 * * *",
            "Daily (midnight)": "0 0 * * *",
            "Daily (6 AM)": "0 6 * * *",
            "Weekly (Sunday midnight)": "0 0 * * 0",
            "Custom": "",
        }
        sched_preset = st.selectbox(
            "Frequency", list(cron_presets.keys()), key="sched_preset"
        )

        if sched_preset == "Custom":
            sched_cron = st.text_input(
                "CRON expression (e.g. 0 */2 * * *)",
                key="sched_custom_cron",
            )
        else:
            sched_cron = cron_presets[sched_preset]
            st.caption(f"CRON: `{sched_cron}`")

        # -- Create button --
        can_create = bool(sched_cron and sched_ws_name)
        if st.button("Create Schedule", type="primary", disabled=not can_create):
            task_name = f"GSHEET_SYNC_{sched_db}_{sched_schema}_{sched_table}".upper()
            fqn = f"{sched_db}.{sched_schema}.{sched_table}"
            with st.spinner("Creating stored procedure, task, and schedule..."):
                try:
                    create_sync_procedure(task_name, fqn, sched_ss_id, sched_ws_name)
                    create_sync_task(task_name, sched_cron)
                    insert_schedule_record(
                        task_name, sched_db, sched_schema, sched_table,
                        sched_ss_id, sched_ss_name, sched_ws_name, sched_cron,
                    )
                    st.success(
                        f"Schedule created. Task **{task_name}** will sync "
                        f"**{fqn}** to **{sched_ss_name} / {sched_ws_name}** "
                        f"on schedule `{sched_cron}`."
                    )
                except Exception as e:
                    st.error(f"Failed to create schedule: {e}")

    # -- Existing schedules --
    st.subheader("Existing schedules")
    try:
        schedules_df = list_schedules()
    except Exception:
        schedules_df = pd.DataFrame()

    if schedules_df.empty:
        st.info("No schedules created yet.")
    else:
        st.dataframe(
            schedules_df[["TASK_NAME", "SOURCE_TABLE", "SPREADSHEET_NAME",
                          "WORKSHEET_NAME", "CRON_EXPRESSION", "STATUS", "CREATED_AT"]],
            use_container_width=True,
        )

        del_task = st.selectbox(
            "Select schedule to delete",
            schedules_df["TASK_NAME"].tolist(),
            key="del_schedule",
        )
        if st.button("Delete selected schedule"):
            row = schedules_df[schedules_df["TASK_NAME"] == del_task].iloc[0]
            with st.spinner(f"Deleting {del_task}..."):
                try:
                    delete_schedule(del_task, int(row["SCHEDULE_ID"]))
                    st.success(f"Schedule **{del_task}** deleted.")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to delete schedule: {e}")


# -- MONITOR TAB --
with tab_monitor:
    st.subheader("Scheduled sync monitor")

    if st.button("Refresh", key="monitor_refresh"):
        st.rerun()

    try:
        mon_schedules = list_schedules()
    except Exception:
        mon_schedules = pd.DataFrame()

    if mon_schedules.empty:
        st.info("No schedules to monitor. Create one in the **Schedule Sync** tab.")
    else:
        st.markdown("**All schedules**")
        st.dataframe(
            mon_schedules[["TASK_NAME", "SOURCE_DATABASE", "SOURCE_SCHEMA",
                           "SOURCE_TABLE", "SPREADSHEET_NAME", "WORKSHEET_NAME",
                           "CRON_EXPRESSION", "STATUS"]],
            use_container_width=True,
        )

        st.subheader("Execution history")
        mon_task = st.selectbox(
            "Select schedule to view history",
            mon_schedules["TASK_NAME"].tolist(),
            key="mon_task_select",
        )

        if mon_task:
            try:
                history_df = get_task_history(mon_task)
            except Exception as e:
                st.error(f"Failed to load task history: {e}")
                history_df = pd.DataFrame()

            if history_df.empty:
                st.info(
                    f"No execution history for **{mon_task}** yet. "
                    "The task will run on its next scheduled time."
                )
            else:
                # Show summary counts
                col1, col2, col3 = st.columns(3)
                total_runs = len(history_df)
                succeeded = len(history_df[history_df["STATE"] == "SUCCEEDED"])
                failed = len(history_df[history_df["STATE"] == "FAILED"])
                col1.metric("Total runs (last 7 days)", total_runs)
                col2.metric("Succeeded", succeeded)
                col3.metric("Failed", failed)

                st.dataframe(history_df, use_container_width=True)
