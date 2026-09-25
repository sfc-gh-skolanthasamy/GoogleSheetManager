from __future__ import annotations

import base64
import json
import re

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# App host (Streamlit app lives here)
SF_DATABASE = "MY_DATA_DB"
SF_SCHEMA = "PUBLIC"
SF_WAREHOUSE = "MY_WAREHOUSE"
SF_EAI_NAME = "GOOGLE_SHEETS_ACCESS_INTEGRATION"
SF_TIMEZONE = "America/New_York"
TASK_NAME_PREFIX = "GSHEET_SYNC"

# Operational schema — tasks, procedures, and schedules table live here
OPS_DATABASE = "MY_DATA_DB"
OPS_SCHEMA = "PUBLIC"

# Secret location
SF_SECRET_DATABASE = SF_DATABASE
SF_SECRET_SCHEMA = SF_SCHEMA
SF_SECRET_NAME = "google_service_account_key"
SF_SECRET_SNOWFLAKE_NAME = "GOOGLE_SERVICE_ACCOUNT_KEY"
SF_SECRET_FQN = f'{SF_SECRET_DATABASE}.{SF_SECRET_SCHEMA}.{SF_SECRET_SNOWFLAKE_NAME}'

# Schedules metadata table
SCHEDULES_TABLE = f"{OPS_DATABASE}.{OPS_SCHEMA}.GSHEET_SYNC_SCHEDULES"

# Limits & cache TTLs (seconds)
MAX_DOWNLOAD_ROWS = 10_000
TASK_HISTORY_DAYS = 7
TASK_HISTORY_LIMIT = 50
CACHE_TTL_SHORT = 30
CACHE_TTL_MEDIUM = 60
CACHE_TTL_DEFAULT = 120
CACHE_TTL_LONG = 300

# App display
APP_TITLE = "Google Sheets Manager"
APP_ICON = "\U0001f4ca"

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MANUAL_URL_OPTION = "\u2709\ufe0f  Paste URL manually..."

# =============================================================================

st.set_page_config(
    page_title=APP_TITLE,
    page_icon=APP_ICON,
    layout="wide",
)

st.session_state.setdefault("selected_worksheet", None)
st.session_state.setdefault("gcp_creds_dict", None)

def _load_creds_from_secret() -> dict | None:
    try:
        import _snowflake
        secret_value = _snowflake.get_generic_secret_string(SF_SECRET_NAME)
        try:
            decoded = base64.b64decode(secret_value).decode("utf-8")
            return json.loads(decoded)
        except Exception:
            pass
        return json.loads(secret_value)
    except ImportError:
        return None
    except Exception as e:
        st.sidebar.error(f"Secret load failed: {e}")
        return None


# -- Google auth --
def get_gspread_client():
    creds_dict = st.session_state.get("gcp_creds_dict")
    if not creds_dict:
        return None
    if "private_key" in creds_dict and "\\n" in creds_dict["private_key"]:
        creds_dict = {**creds_dict, "private_key": creds_dict["private_key"].replace("\\n", "\n")}
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    return gspread.authorize(creds)


@st.cache_resource
def get_snowpark_session():
    from snowflake.snowpark.context import get_active_session
    return get_active_session()


# -- Data loading --

@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def extract_spreadsheet_id(url: str) -> str | None:
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", url)
    if match:
        return match.group(1)
    stripped = url.strip()
    if stripped and "/" not in stripped and re.fullmatch(r"[a-zA-Z0-9_-]+", stripped):
        return stripped
    return None


@st.cache_data(ttl=CACHE_TTL_LONG)
def list_available_sheets() -> list[dict]:
    """List all Google Sheets accessible by the service account."""
    client = get_gspread_client()
    if not client:
        return []
    try:
        all_sheets = client.openall()
        return [
            {"title": s.title, "id": s.id, "url": s.url}
            for s in sorted(all_sheets, key=lambda s: s.title.lower())
        ]
    except Exception:
        return []


@st.cache_data(ttl=CACHE_TTL_DEFAULT)
def get_spreadsheet_info(spreadsheet_id: str) -> dict:
    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    return {
        "name": spreadsheet.title,
        "id": spreadsheet.id,
        "worksheets": [ws.title for ws in spreadsheet.worksheets()],
    }


@st.cache_data(ttl=CACHE_TTL_MEDIUM)
def load_worksheet(spreadsheet_id: str, worksheet_name: str) -> pd.DataFrame:
    client = get_gspread_client()
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.worksheet(worksheet_name)
    try:
        records = worksheet.get_all_records()
    except IndexError:
        values = worksheet.get_all_values()
        if not values or len(values) < 2:
            return pd.DataFrame()
        headers = values[0]
        return pd.DataFrame(values[1:], columns=headers)
    if not records:
        return pd.DataFrame()
    return pd.DataFrame(records)


@st.cache_data(ttl=CACHE_TTL_LONG)
def get_table_columns(table_name: str) -> list[dict]:
    session = get_snowpark_session()
    result = session.sql(f"DESCRIBE TABLE IDENTIFIER('{table_name}')").collect()
    columns = []
    for row in result:
        row_dict = row.as_dict() if hasattr(row, "as_dict") else dict(row)
        name = row_dict.get("name") or row_dict.get("NAME")
        col_type = row_dict.get("type") or row_dict.get("TYPE")
        if name:
            columns.append({"name": name, "type": col_type or "UNKNOWN"})
    return columns


def upload_to_snowflake(df: pd.DataFrame, table_name: str, column_mapping: dict,
                        database: str, schema: str):
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


def _infer_snowflake_type(series: pd.Series) -> str:
    if series.dropna().empty:
        return "VARCHAR"
    if pd.api.types.is_integer_dtype(series):
        return "NUMBER"
    if pd.api.types.is_float_dtype(series):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "VARCHAR"


def create_table_from_df(df: pd.DataFrame, table_name: str, database: str, schema: str):
    session = get_snowpark_session()
    col_defs = []
    for col in df.columns:
        safe_col = col.upper().replace(" ", "_")
        safe_col = re.sub(r"[^A-Z0-9_]", "", safe_col)
        if not safe_col:
            safe_col = f"COL_{df.columns.tolist().index(col)}"
        sf_type = _infer_snowflake_type(df[col])
        col_defs.append(f'"{safe_col}" {sf_type}')
    cols_sql = ", ".join(col_defs)
    fqn = f"{database}.{schema}.{table_name}"
    session.sql(f"CREATE TABLE {fqn} ({cols_sql})").collect()


def read_snowflake_table(fqn: str, limit: int = MAX_DOWNLOAD_ROWS,
                         where_clause: str = "") -> pd.DataFrame:
    session = get_snowpark_session()
    df = session.table(fqn)
    if where_clause:
        df = df.filter(where_clause)
    return df.limit(limit).to_pandas()


@st.cache_data(ttl=CACHE_TTL_LONG)
def list_databases() -> list[str]:
    session = get_snowpark_session()
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]


@st.cache_data(ttl=CACHE_TTL_LONG)
def list_schemas(database: str) -> list[str]:
    session = get_snowpark_session()
    result = session.sql(f"SHOW SCHEMAS IN DATABASE IDENTIFIER('{database}')").collect()
    return [row["name"] for row in result]


@st.cache_data(ttl=CACHE_TTL_LONG)
def list_tables_in(database: str, schema: str) -> list[str]:
    session = get_snowpark_session()
    result = session.sql(
        f"SHOW TABLES IN SCHEMA IDENTIFIER('{database}.{schema}')"
    ).collect()
    return [row["name"] for row in result]


def write_to_worksheet(spreadsheet_id: str, worksheet_name: str, df: pd.DataFrame, create_new: bool):
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

def _sanitize_for_proc_body(value: str) -> str:
    if "$$" in value:
        raise ValueError(f"Value must not contain '$$': {value!r}")
    return value.replace("\\", "\\\\").replace('"', '\\"')


def create_sync_procedure(task_name: str, fqn: str, spreadsheet_id: str,
                          worksheet_name: str, where_clause: str = "",
                          row_limit: int = MAX_DOWNLOAD_ROWS):
    safe_task = _validate_identifier(task_name)
    fqn_parts = fqn.split(".")
    if len(fqn_parts) != 3 or not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in fqn_parts):
        raise ValueError(f"Invalid fully-qualified table name: {fqn!r}")
    safe_fqn = ".".join(fqn_parts)
    safe_ss_id = _sanitize_for_proc_body(spreadsheet_id)
    safe_ws_name = _sanitize_for_proc_body(worksheet_name)
    safe_where = _sanitize_for_proc_body(where_clause) if where_clause else ""
    safe_limit = int(row_limit)
    where_clause_encoded = str([ord(c) for c in safe_where])

    session = get_snowpark_session()
    proc_sql = f"""
CREATE OR REPLACE PROCEDURE {OPS_DATABASE}.{OPS_SCHEMA}.{safe_task}_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ("snowflake-snowpark-python", "gspread", "google-auth")
HANDLER = 'run'
EXTERNAL_ACCESS_INTEGRATIONS = ({SF_EAI_NAME})
SECRETS = ('{SF_SECRET_NAME}' = {SF_SECRET_FQN})
AS
$$
import json
import _snowflake
import gspread
from google.oauth2.service_account import Credentials
from snowflake.snowpark.context import get_active_session

def run(session=None):
    if session is None:
        session = get_active_session()
    secret_value = _snowflake.get_generic_secret_string("{SF_SECRET_NAME}")
    creds_dict = json.loads(secret_value)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    df = session.table("{safe_fqn}")
    where_clause = chr(0)[:0].join(chr(c) for c in {where_clause_encoded})
    if where_clause:
        df = df.filter(where_clause)
    df = df.limit({safe_limit}).to_pandas()
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
    safe_task = _validate_identifier(task_name)
    if not re.fullmatch(r"[0-9 */,\-]+", cron_expr):
        raise ValueError(f"Invalid CRON expression: {cron_expr!r}")
    session = get_snowpark_session()
    session.sql(f"""
CREATE OR REPLACE TASK {OPS_DATABASE}.{OPS_SCHEMA}.{safe_task}
    WAREHOUSE = {SF_WAREHOUSE}
    SCHEDULE = 'USING CRON {cron_expr} {SF_TIMEZONE}'
AS
    CALL {OPS_DATABASE}.{OPS_SCHEMA}.{safe_task}_PROC()
""").collect()
    session.sql(f"ALTER TASK {OPS_DATABASE}.{OPS_SCHEMA}.{safe_task} RESUME").collect()


def insert_schedule_record(task_name, src_db, src_schema, src_table,
                           spreadsheet_id, spreadsheet_name, worksheet_name, cron_expr,
                           where_clause: str = "", row_limit: int = MAX_DOWNLOAD_ROWS):
    session = get_snowpark_session()
    for col_def in [
        "WHERE_CLAUSE VARCHAR DEFAULT ''",
        f"ROW_LIMIT NUMBER DEFAULT {MAX_DOWNLOAD_ROWS}",
    ]:
        try:
            session.sql(f"ALTER TABLE {SCHEDULES_TABLE} ADD COLUMN {col_def}").collect()
        except Exception:
            pass
    session.sql(
        f"INSERT INTO {SCHEDULES_TABLE}"
        " (TASK_NAME, SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE,"
        "  SPREADSHEET_ID, SPREADSHEET_NAME, WORKSHEET_NAME, CRON_EXPRESSION,"
        "  WHERE_CLAUSE, ROW_LIMIT)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params=[task_name, src_db, src_schema, src_table,
                spreadsheet_id, spreadsheet_name, worksheet_name, cron_expr,
                where_clause, row_limit],
    ).collect()


@st.cache_data(ttl=CACHE_TTL_SHORT)
def list_schedules() -> pd.DataFrame:
    session = get_snowpark_session()
    return session.table(SCHEDULES_TABLE).to_pandas()


def _validate_identifier(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return name


def delete_schedule(task_name: str, schedule_id: int):
    safe_name = _validate_identifier(task_name)
    session = get_snowpark_session()
    try:
        session.sql(f"ALTER TASK {OPS_DATABASE}.{OPS_SCHEMA}.{safe_name} SUSPEND").collect()
    except Exception:
        pass
    try:
        session.sql(f"DROP TASK IF EXISTS {OPS_DATABASE}.{OPS_SCHEMA}.{safe_name}").collect()
    except Exception:
        pass
    try:
        session.sql(f"DROP PROCEDURE IF EXISTS {OPS_DATABASE}.{OPS_SCHEMA}.{safe_name}_PROC()").collect()
    except Exception:
        pass
    session.sql(
        f"DELETE FROM {SCHEDULES_TABLE} WHERE SCHEDULE_ID = ?",
        params=[schedule_id],
    ).collect()


def get_task_history(task_name: str) -> pd.DataFrame:
    safe_name = _validate_identifier(task_name)
    session = get_snowpark_session()
    result = session.sql(f"""
SELECT SCHEDULED_TIME, QUERY_START_TIME, COMPLETED_TIME, STATE, ERROR_MESSAGE, RETURN_VALUE
FROM TABLE({OPS_DATABASE}.INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => '{safe_name}',
    SCHEDULED_TIME_RANGE_START => DATEADD('day', -{TASK_HISTORY_DAYS}, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => {TASK_HISTORY_LIMIT}
))
ORDER BY SCHEDULED_TIME DESC
""").collect()
    if not result:
        return pd.DataFrame()
    return pd.DataFrame([row.as_dict() for row in result])


# -- UI --
st.title(APP_TITLE)

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
            "If running locally, check that a [connections.snowflake] section is configured "
            "in .streamlit/secrets.toml."
        )
        st.stop()

    st.divider()
    st.header("Google Drive")

    if st.session_state.get("gcp_creds_dict") is None:
        secret_creds = _load_creds_from_secret()
        if secret_creds:
            st.session_state["gcp_creds_dict"] = secret_creds

    if st.session_state.get("gcp_creds_dict") is None:
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
        if client is None:
            raise ValueError("Failed to create gspread client")
        st.success("Connected to Google")
        st.caption(f"Service account: `{sa_email}`")
    except Exception as e:
        st.error(f"Google auth failed: {e}")
        st.stop()

    st.divider()

    # ---- Google Sheet selection: dropdown of available sheets ----
    with st.spinner("Loading available Google Sheets..."):
        available_sheets = list_available_sheets()

    if available_sheets:
        sheet_options = [f"{s['title']}" for s in available_sheets]
        sheet_options.append(MANUAL_URL_OPTION)

        selected_option = st.selectbox(
            "Select a Google Sheet",
            sheet_options,
            key="sheet_selector",
        )

        if selected_option == MANUAL_URL_OPTION:
            sheet_url = st.text_input(
                "Google Sheet URL",
                placeholder="https://docs.google.com/spreadsheets/d/.../edit",
                key="sheet_url_input",
            )
        else:
            idx = sheet_options.index(selected_option)
            chosen = available_sheets[idx]
            sheet_url = chosen["url"]
            st.caption(f"ID: `{chosen['id']}`")
    else:
        st.warning("No sheets found for this service account. Paste a URL instead.")
        sheet_url = st.text_input(
            "Google Sheet URL",
            placeholder="https://docs.google.com/spreadsheets/d/.../edit",
            key="sheet_url_input",
        )

    if not sheet_url:
        st.info(
            "Select a Google Sheet above to get started. "
            f"Make sure sheets are shared with **{sa_email}** (Editor access)."
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

        if upload_ok:
            CREATE_NEW_OPTION = "-- Create new table --"
            table_options = [CREATE_NEW_OPTION] + tables
            target_table = st.selectbox("Table", table_options, key="up_table")

            creating_new_table = (target_table == CREATE_NEW_OPTION)

            if creating_new_table:
                new_table_name = st.text_input(
                    "New table name",
                    placeholder="e.g. MY_NEW_TABLE",
                    key="up_new_table_name",
                ).strip().upper()

                if new_table_name and not re.fullmatch(r"[A-Z_][A-Z0-9_]*", new_table_name):
                    st.error("Invalid table name. Use letters, numbers, and underscores only (must start with a letter or underscore).")
                    upload_ok = False

                if upload_ok and new_table_name:
                    up_fqn = f"{up_database}.{up_schema}.{new_table_name}"

                    st.subheader("Inferred column definitions")
                    st.caption("Columns will be created based on the sheet data. All columns default to VARCHAR unless numeric/date types are detected.")

                    col_info = []
                    for col in df.columns:
                        safe_col = col.upper().replace(" ", "_")
                        safe_col = re.sub(r"[^A-Z0-9_]", "", safe_col)
                        if not safe_col:
                            safe_col = f"COL_{df.columns.tolist().index(col)}"
                        sf_type = _infer_snowflake_type(df[col])
                        col_info.append({"Sheet Column": col, "Snowflake Column": safe_col, "Type": sf_type})

                    st.dataframe(pd.DataFrame(col_info), use_container_width=True)

                    if st.button("Create Table & Upload", type="primary"):
                        with st.spinner(f"Creating table {up_fqn} and uploading {len(df)} rows..."):
                            try:
                                create_table_from_df(df, new_table_name, up_database, up_schema)
                                col_mapping = {}
                                for col in df.columns:
                                    safe_col = col.upper().replace(" ", "_")
                                    safe_col = re.sub(r"[^A-Z0-9_]", "", safe_col)
                                    if not safe_col:
                                        safe_col = f"COL_{df.columns.tolist().index(col)}"
                                    col_mapping[col] = safe_col
                                upload_to_snowflake(df, new_table_name, col_mapping,
                                                    up_database, up_schema)
                                st.success(
                                    f"Created table and uploaded {len(df)} rows to **{up_fqn}**."
                                )
                            except Exception as e:
                                st.error(f"Upload failed: {e}")
            else:
                up_fqn = f"{up_database}.{up_schema}.{target_table}"
                st.subheader("Column mapping")

                try:
                    sf_columns = get_table_columns(up_fqn)
                except Exception as e:
                    st.error(f"Failed to get table columns: {e}")
                    sf_columns = []
                    upload_ok = False

                if upload_ok and sf_columns:
                    sf_col_names = [c["name"] for c in sf_columns]
                    column_mapping = {}
                    for sheet_col in df.columns:
                        best_match_idx = 0
                        clean_sheet = sheet_col.upper().replace(" ", "_")
                        clean_sheet = re.sub(r"[^A-Z0-9_]", "", clean_sheet)
                        for i, sf_name in enumerate(sf_col_names):
                            if sf_name.upper() == clean_sheet:
                                best_match_idx = i
                                break
                        mapped = st.selectbox(
                            f"**{sheet_col}** maps to",
                            sf_col_names,
                            index=best_match_idx,
                            key=f"map_{sheet_col}",
                        )
                        column_mapping[sheet_col] = mapped

                    if st.button("Upload to Snowflake", type="primary"):
                        with st.spinner(f"Uploading {len(df)} rows to **{up_fqn}**..."):
                            try:
                                upload_to_snowflake(df, target_table, column_mapping,
                                                    up_database, up_schema)
                                st.success(f"Uploaded {len(df)} rows to **{up_fqn}**.")
                            except Exception as e:
                                st.error(f"Upload failed: {e}")


# -- DOWNLOAD TO GOOGLE SHEET TAB --
with tab_download:
    st.subheader("Download Snowflake table to Google Sheet")

    dl_ok = True
    try:
        dl_databases = list_databases()
    except Exception as e:
        st.error(f"Failed to list databases: {e}")
        dl_databases = []
        dl_ok = False

    if dl_ok and not dl_databases:
        st.warning("No databases found.")
        dl_ok = False

    if dl_ok:
        dl_database = st.selectbox("Database", dl_databases, key="dl_database")

        try:
            dl_schemas = list_schemas(dl_database)
        except Exception as e:
            st.error(f"Failed to list schemas: {e}")
            dl_schemas = []
            dl_ok = False

    if dl_ok and not dl_schemas:
        st.warning(f"No schemas found in {dl_database}.")
        dl_ok = False

    if dl_ok:
        dl_schema = st.selectbox("Schema", dl_schemas, key="dl_schema")

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
        dl_table = st.selectbox("Table", dl_tables, key="dl_table")
        dl_fqn = f"{dl_database}.{dl_schema}.{dl_table}"

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
            placeholder="e.g. STATUS = 'ACTIVE' AND CREATED_AT > '2024-01-01'",
            key="dl_where_clause",
            help="Filter rows before downloading. Enter a SQL boolean expression (without the WHERE keyword).",
        )

        st.subheader("Target worksheet")

        target_ws_option = st.radio(
            "Write to",
            ["Existing worksheet", "New worksheet", "New spreadsheet"],
            key="dl_ws_option",
        )

        if target_ws_option == "New worksheet":
            target_ws_name = st.text_input(
                "New worksheet name",
                value=dl_table,
                key="dl_new_ws",
            )
            target_create_new = True
            target_new_ss = False
        elif target_ws_option == "New spreadsheet":
            new_ss_title = st.text_input(
                "New spreadsheet title",
                value=f"{dl_table} export",
                key="dl_new_ss_title",
            )
            target_ws_name = st.text_input(
                "Worksheet name",
                value=dl_table,
                key="dl_new_ss_ws",
            )
            target_create_new = True
            target_new_ss = True
        else:
            target_ws_name = st.selectbox(
                "Select worksheet",
                ss_info["worksheets"],
                key="dl_existing_ws",
            )
            target_create_new = False
            target_new_ss = False

        if st.button("Download & Write", type="primary"):
            with st.spinner(f"Reading up to {dl_row_limit:,} rows from **{dl_fqn}**..."):
                try:
                    dl_df = read_snowflake_table(dl_fqn, limit=dl_row_limit,
                                                 where_clause=dl_where)
                except Exception as e:
                    st.error(f"Failed to read table: {e}")
                    dl_df = pd.DataFrame()

            if not dl_df.empty:
                with st.spinner("Writing to Google Sheets..."):
                    try:
                        if target_new_ss:
                            result = create_new_spreadsheet(
                                new_ss_title, target_ws_name, dl_df,
                                share_with_email=None,
                            )
                            st.success(
                                f"Created new spreadsheet with {len(dl_df)} rows. "
                                f"[Open in Google Sheets]({result['url']})"
                            )
                        else:
                            write_to_worksheet(ss_id, target_ws_name, dl_df, target_create_new)
                            st.success(
                                f"Wrote {len(dl_df)} rows to **{ss_info['name']} / {target_ws_name}**."
                            )
                    except Exception as e:
                        st.error(f"Failed to write to Google Sheets: {e}")
            elif dl_df.empty:
                st.info("No data found matching the criteria.")


# -- SCHEDULE SYNC TAB --
with tab_schedule:
    st.subheader("Create a scheduled sync")
    st.caption("Automatically sync a Snowflake table to a Google Sheet on a recurring schedule.")

    sched_ok = True
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

        sched_row_limit = st.number_input(
            "Max rows to sync",
            min_value=1,
            max_value=MAX_DOWNLOAD_ROWS,
            value=MAX_DOWNLOAD_ROWS,
            step=1000,
            key="sched_row_limit",
            help=f"Maximum allowed: {MAX_DOWNLOAD_ROWS:,} rows",
        )

        sched_where = st.text_input(
            "WHERE clause (optional)",
            placeholder="e.g. STATUS = 'ACTIVE' AND CREATED_AT > '2024-01-01'",
            key="sched_where_clause",
            help="Filter rows before syncing. Enter a SQL boolean expression (without the WHERE keyword).",
        )

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
            task_name = f"{TASK_NAME_PREFIX}_{sched_db}_{sched_schema}_{sched_table}".upper()
            fqn = f"{sched_db}.{sched_schema}.{sched_table}"
            with st.spinner("Creating stored procedure, task, and schedule..."):
                try:
                    create_sync_procedure(task_name, fqn, sched_ss_id, sched_ws_name,
                                          where_clause=sched_where, row_limit=sched_row_limit)
                    create_sync_task(task_name, sched_cron)
                    insert_schedule_record(
                        task_name, sched_db, sched_schema, sched_table,
                        sched_ss_id, sched_ss_name, sched_ws_name, sched_cron,
                        where_clause=sched_where, row_limit=sched_row_limit,
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
        sched_display_cols = ["TASK_NAME", "SOURCE_TABLE", "SPREADSHEET_NAME",
                              "WORKSHEET_NAME", "CRON_EXPRESSION", "STATUS", "CREATED_AT",
                              "WHERE_CLAUSE", "ROW_LIMIT"]
        sched_display_cols = [c for c in sched_display_cols if c in schedules_df.columns]
        st.dataframe(
            schedules_df[sched_display_cols],
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
                col1, col2, col3 = st.columns(3)
                total_runs = len(history_df)
                succeeded = len(history_df[history_df["STATE"] == "SUCCEEDED"])
                failed = len(history_df[history_df["STATE"] == "FAILED"])
                col1.metric(f"Total runs (last {TASK_HISTORY_DAYS} days)", total_runs)
                col2.metric("Succeeded", succeeded)
                col3.metric("Failed", failed)

                st.dataframe(history_df, use_container_width=True)
