import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import json
import altair as alt
import numpy as np
import re # For tag alias cleaning
import logging
import tempfile
import os
import datetime as dt
import yaml
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException



# --- Initialize Snowpark Session from Streamlit in Snowflake ---
def get_snowpark_session_sis():
    try:
        return Session.builder.getOrCreate()
    except Exception as e:
        st.error(f"Error getting Snowpark session in Streamlit in Snowflake: {e}")
        return None

def get_snowflake_environment_sis(session: Session):
    try:
        # Fetch initial environment. We'll use these as defaults.
        results = session.sql("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_ACCOUNT()").collect()[0]
        return {
            "user": results[0],
            "warehouse": results[1] if results[1] else "", # Handle None for warehouse
            "database": results[2] if results[2] else "",
            "schema": results[3] if results[3] else "",
            "role": results[4],
            "account": results[5],
        }
    except Exception as e:
        st.error(f"Error getting initial Snowflake environment info: {e}")
        return None

def ensure_named_stage_exists(session, db_name: str, schema_name: str, stage_name: str):
    """
    Checks if a named stage exists and creates it if it doesn't.
    Grants USAGE on schema and stage to PUBLIC for shared access.
    """
    logging.info(f"Ensuring stage {db_name}.{schema_name}.{stage_name} exists...")
    try:
        # Create schema, fully qualifying its name
        session.sql(f'CREATE SCHEMA IF NOT EXISTS "{db_name}"."{schema_name}"').collect()
        session.sql(f'GRANT USAGE ON SCHEMA "{db_name}"."{schema_name}" TO PUBLIC').collect()
        st.info(f"Schema `{db_name}.{schema_name}` ensured to exist and USAGE granted to PUBLIC.")

        # Create stage, fully qualifying its name
        session.sql(f'CREATE STAGE IF NOT EXISTS "{db_name}"."{schema_name}"."{stage_name}" DIRECTORY = (ENABLE = TRUE)').collect()
        session.sql(f'GRANT READ ON STAGE "{db_name}"."{schema_name}"."{stage_name}" TO PUBLIC').collect()
        session.sql(f'GRANT READ ON STAGE "{db_name}"."{schema_name}"."{stage_name}" TO ROLE {session.get_current_role()}').collect()
        session.sql(f'GRANT WRITE ON STAGE "{db_name}"."{schema_name}"."{stage_name}" TO ROLE {session.get_current_role()}').collect()
        st.success(f"Named stage `{db_name}.{schema_name}.{stage_name}` ensured to exist and privileges granted.")
        return True
    except Exception as e:
        st.error(f"Error ensuring named stage '{db_name}.{schema_name}.{stage_name}': {e}")
        st.error("Please ensure the active role has CREATE SCHEMA and CREATE STAGE privileges in the specified database/schema.")
        return False

# --- Schema Introspection and YAML Generation ---

def get_schema_details(session, db_name: str, schema_name: str) -> list:
    """Queries INFORMATION_SCHEMA to get details of tables and views."""
    entities = []
    query_timeout_ms = 30000
    upper_schema_name = schema_name.upper()

    logging.info(f"Starting schema retrieval for {db_name}.{upper_schema_name}")
    
    # Try fetching tables
    try:
        tables_query = f"""
            SELECT TABLE_NAME, COMMENT, LAST_ALTERED, CREATED
            FROM "{db_name}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{upper_schema_name}' AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME;
        """
        tables = session.sql(tables_query).collect(statement_params={"QUERY_TIMEOUT_IN_SECONDS": query_timeout_ms // 1000})
        for tbl_row in tables:
            entities.append({
                "name": tbl_row["TABLE_NAME"],
                "type": "TABLE",
                "description": tbl_row["COMMENT"] or "",
                "last_modified_date": str(tbl_row["LAST_ALTERED"]) if tbl_row["LAST_ALTERED"] else None,
                "creation_date": str(tbl_row["CREATED"]) if tbl_row["CREATED"] else None
            })
    except SnowparkSQLException as e:
        st.warning(f"Could not retrieve tables for `{db_name}`.`{upper_schema_name}`: {e.message}")
    except Exception as e:
        st.warning(f"An unexpected error occurred while retrieving tables for `{db_name}`.`{upper_schema_name}`: {e}")

    # Try fetching views
    try:
        views_query = f"""
            SELECT TABLE_NAME, COMMENT, LAST_ALTERED, CREATED
            FROM "{db_name}".INFORMATION_SCHEMA.VIEWS
            WHERE TABLE_SCHEMA = '{upper_schema_name}'
            ORDER BY TABLE_NAME;
        """
        views = session.sql(views_query).collect(statement_params={"QUERY_TIMEOUT_IN_SECONDS": query_timeout_ms // 1000})
        for vw_row in views:
            entities.append({
                "name": vw_row["TABLE_NAME"],
                "type": "VIEW",
                "description": vw_row["COMMENT"] or "",
                "last_modified_date": str(vw_row["LAST_ALTERED"]) if vw_row["LAST_ALTERED"] else None,
                "creation_date": str(vw_row["CREATED"]) if vw_row["CREATED"] else None
            })
    except SnowparkSQLException as e:
        st.warning(f"Could not retrieve views for `{db_name}`.`{upper_schema_name}`: {e.message}")
    except Exception as e:
        st.warning(f"An unexpected error occurred while retrieving views for `{db_name}`.`{upper_schema_name}`: {e}")

    # Try fetching materialized views
    try:
        mvs_query = f"""
            SELECT TABLE_NAME, COMMENT, LAST_ALTERED, CREATED
            FROM "{db_name}".INFORMATION_SCHEMA.MATERIALIZED_VIEWS
            WHERE TABLE_SCHEMA = '{upper_schema_name}'
            ORDER BY TABLE_NAME;
        """
        mvs = session.sql(mvs_query).collect(statement_params={"QUERY_TIMEOUT_IN_SECONDS": query_timeout_ms // 1000})
        for mv_row in mvs:
            entities.append({
                "name": mv_row["TABLE_NAME"],
                "type": "MATERIALIZED VIEW",
                "description": mv_row["COMMENT"] or "",
                "last_modified_date": str(mv_row["LAST_ALTERED"]) if mv_row["LAST_ALTERED"] else None,
                "creation_date": str(mv_row["CREATED"]) if mv_row["CREATED"] else None
            })
    except SnowparkSQLException as e:
        st.warning(f"Could not retrieve materialized views for `{db_name}`.`{upper_schema_name}`: {e.message}")
    except Exception as e:
        st.warning(f"An unexpected error occurred while retrieving materialized views for `{db_name}`.`{upper_schema_name}`: {e}")


    for entity in entities:
        entity_name = entity["name"]
        columns = []
        try:
            columns_query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, COMMENT
                FROM "{db_name}".INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = '{upper_schema_name}' AND TABLE_NAME = '{entity_name}'
                ORDER BY ORDINAL_POSITION;
            """
            cols_data = session.sql(columns_query).collect(statement_params={"QUERY_TIMEOUT_IN_SECONDS": query_timeout_ms // 1000})
            for col_row in cols_data:
                columns.append({
                    "name": col_row["COLUMN_NAME"],
                    "datatype": col_row["DATA_TYPE"],
                    "description": col_row["COMMENT"] or ""
                })
            entity["columns"] = columns
        except SnowparkSQLException as e:
            st.warning(f"Could not retrieve columns for `{db_name}`.`{upper_schema_name}`.`{entity_name}`: {e.message}")
            entity["columns"] = []
        except Exception as e:
            st.warning(f"An unexpected error occurred while retrieving columns for `{db_name}`.`{upper_schema_name}`.`{entity_name}`: {e}")
            entity["columns"] = []
    return entities

def generate_semantic_model_yaml_string(session, db_name: str, schema_name: str) -> str | None:
    """Generates a YAML string representing the schema of the target database and schema."""
    st.write(f"Generating YAML for: `{db_name}`.`{schema_name}`...")
    schema_entities = get_schema_details(session, db_name, schema_name)
    if not schema_entities:
        st.error(f"No tables or views found (or accessible) in `{db_name}`.`{schema_name}` to generate YAML.")
        return None

    yaml_data = {
        "version": "1.0",
        "description": f"Dynamically generated semantic model for {db_name}.{schema_name}",
        "database_name": db_name,
        "schema_name": schema_name,
        "entities": schema_entities
    }
    try:
        yaml_string = yaml.dump(yaml_data, sort_keys=False, indent=2, allow_unicode=True)
        st.success(f"Successfully generated YAML representation for `{db_name}`.`{schema_name}`.")
        return yaml_string
    except Exception as e:
        st.error(f"Error converting schema to YAML: {e}")
        return None

def upload_yaml_to_named_stage(session, yaml_string: str, file_name: str) -> str | None:
    """Uploads a YAML string to the specified named stage."""
    staged_file_path_for_api = f"@{selected_database}.{selected_schema}.{YAML_STAGE_NAME}/{file_name}"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False, encoding='utf-8') as tmp_file:
        tmp_file.write(yaml_string)
        local_file_path = tmp_file.name

    logging.info(f"Uploading local file {local_file_path} to named stage path: {staged_file_path_for_api}")
    try:
        # Use Snowpark's file.put for uploading to a named stage
        session.file.put(local_file_path, staged_file_path_for_api, auto_compress=False, overwrite=True)
        st.success(f"YAML uploaded to named stage: `{staged_file_path_for_api}`")
        logging.info(f"YAML successfully uploaded to {staged_file_path_for_api}")
        return staged_file_path_for_api
    except Exception as e:
        st.error(f"Error uploading YAML to named stage (`{staged_file_path_for_api}`): {e}")
        logging.error(f"Error during YAML upload to named stage: {e}")
        st.error("Please ensure the Snowflake user has WRITE privileges on the named stage.")
        return None
    finally:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)

# --- Core Functions (send_message_to_analyst, process_analyst_message, display_analyst_content) ---


    

def send_message_to_analyst(session: Session, prompt_text: str) -> dict:
    """
    Sends a text prompt to a Snowflake Cortex LLM (e.g., snowflake-arctic)
    to get a text-based response for analysis/suggestions.
    """
    try:
        
        # If get_active_session() works, assume _snowflake is implicitly available for Cortex functions

        escaped_prompt = prompt_text.replace("'", "''") # Escape for SQL string literal
        
        # Call Cortex COMPLETE function directly via Snowpark
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{escaped_prompt}') AS response_text").collect()
        
        if result and result[0]['RESPONSE_TEXT']:
            return {"message": {"content": [{"type": "text", "text": result[0]['RESPONSE_TEXT']}]}}
        else:
            return {"message": {"content": [{"type": "text", "text": "Cortex LLM returned an empty response."}]}}
    except Exception as e:
        logging.error(f"Exception calling Cortex LLM: {e}")
        st.error(f"Error calling Cortex LLM for analysis: {e}")
        return {"error": f"Error calling Cortex LLM: {e}"}

def process_analyst_message(session: Session, prompt: str, semantic_model_path_for_info: str = None) -> None:
    """
    Processes a prompt, sends it to the Cortex LLM, and displays the response.
    semantic_model_path_for_info is just for display, not directly used by LLM.
    """
    if not prompt:
        st.error("Prompt cannot be empty.")
        return
    try:
        st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
        )
    except Exception as e: 
        st.error(f"error getting access to session {e}")

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Cortex LLM is thinking... This may take a moment for complex analysis."):
            try:
                # Call the updated send_message_to_analyst function
                response = send_message_to_analyst(session=session, prompt_text=prompt)
                
                if "error" in response:
                    st.error(f"LLM processing failed: {response['error']}")
                else:
                    content = response["message"]["content"]
                    display_analyst_content(content=content)
                    st.session_state.messages.append({"role": "assistant", "content": content})
            except Exception as e:
                st.error(f"Failed to get full response from LLM due to an earlier error: {e}")

def display_analyst_content(content: list, message_index: int = None) -> None:
    effective_message_index = message_index if message_index is not None else len(st.session_state.get("messages", []))
    for item_index, item in enumerate(content):
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions": # This type might not be directly returned by COMPLETE, but kept for compatibility
            suggestion_expander_key = f"expander_sugg_{effective_message_index}_{item_index}"
            with st.expander("Suggestions", expanded=True, key=suggestion_expander_key):
                for suggestion_index, suggestion_text in enumerate(item["suggestions"]):
                    # If suggestions are plain text, just display them.
                    # If they were structured (e.g., from former API), you might parse them.
                    st.markdown(f"- {suggestion_text}")
        elif item["type"] == "sql":
            sql_expander_key = f"expander_sql_{effective_message_index}_{item_index}"
            results_expander_key = f"expander_results_{effective_message_index}_{item_index}"
            with st.expander("SQL Query", expanded=False, key=sql_expander_key):
                st.code(item["statement"], language="sql")
            try:
                with st.expander("Results", expanded=True, key=results_expander_key):
                    with st.spinner("Running SQL..."):
                        session = get_active_session() # Get fresh session for direct SQL execution
                        df = session.sql(item["statement"]).to_pandas()
                        if not df.empty:
                            st.dataframe(df)
                        else:
                            st.write("SQL query ran successfully but returned no data.")
            except Exception as e:
                st.error(f"Failed to execute or display SQL query results: {e}")
        else:
            st.write(f"Unsupported content type: {item['type']}")
            st.json(item)

# --- Utility Functions ---
def fetch_data(session, query):
    """Executes a SQL query and returns a Pandas DataFrame."""
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error executing query: {query}\n{e}")
        return pd.DataFrame()

def analyze_text_with_cortex(session, text, prompt_suffix):
    """Analyzes text using Snowflake Cortex Analyst with a dynamic prompt."""
    try:
        # Ensure text is properly escaped for SQL string literal
        escaped_text = text.replace("'", "''")
        full_prompt = f"""Analyze the following text and provide insights based on the Snowflake official documentation regarding {prompt_suffix}:\n\n```text\n{escaped_text}\n```\n"""
        escaped_full_prompt = full_prompt.replace("'", "''")
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{escaped_full_prompt}') AS analysis").collect() # Using COMPLETE as ANALYZE_TEXT is more opaque
        if result and result[0]['ANALYSIS']:
            # Cortex COMPLETE returns a string, not necessarily JSON. We'll return the raw string.
            # If you expect JSON, you might need to adjust the prompt and parse it.
            return {"suggestions": result[0]['ANALYSIS']}
        else:
            return {"suggestions": f"No specific insights found or empty response from Cortex regarding {prompt_suffix}."}
    except Exception as e:
        return {"error": f"Error during Cortex analysis for {prompt_suffix}: {e}"}

def analyze_sql_with_cortex(session, sql_query):
    """Analyzes SQL using Snowflake Cortex for style and organization."""
    try:
        escaped_sql_query = sql_query.replace("'", "''")
        prompt = f"""
        Analyze the following SQL query for organization, writing style, and variable naming.
        Suggest improvements based on SQL best practices. Output should be a list of suggestions.

        ```sql
        {escaped_sql_query}
        ```
        """
        escaped_prompt = prompt.replace("'", "''")
        # Using snowflake-arctic for potentially better structured output or more control
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{escaped_prompt}') AS analysis").collect()
        if result and result[0]['ANALYSIS']:
            return {"suggestions": result[0]['ANALYSIS']} # Raw output from COMPLETE
        else:
            return {"suggestions": "No specific style/organization suggestions found from Cortex."}
    except Exception as e:
        return {"error": f"Error during Cortex SQL analysis: {e}"}

def analyze_python_with_cortex(session, python_code):
    """Analyzes Python using Snowflake Cortex for style and organization."""
    try:
        escaped_python_code = python_code.replace("'", "''")
        prompt = f"""
        Analyze the following Python code for organization, writing style, and variable naming.
        Suggest improvements based on Python best practices (like PEP 8). Output should be a list of suggestions.

        ```python
        {escaped_python_code}
        ```
        """
        escaped_prompt = prompt.replace("'", "''")
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.COMPLETE('snowflake-arctic', '{escaped_prompt}') AS analysis").collect()
        if result and result[0]['ANALYSIS']:
            return {"suggestions": result[0]['ANALYSIS']} # Raw output from COMPLETE
        else:
            return {"suggestions": "No specific style/organization suggestions found from Cortex."}
    except Exception as e:
        return {"error": f"Error during Cortex Python analysis: {e}"}

def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = (
        {}
    )  # Dictionary to store feedback submission for each request

def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_analyst_content(content=content)
            
# --- Streamlit App ---
st.set_page_config(layout="wide")
st.title("Snowflake Database Audit Tool ❄️")


if st.session_state.messages is None:
     # re initialise variable 
    reset_session_state()
    
display_conversation()

# check active session 
session = get_snowpark_session_sis() # Re-get session for audit run
if not session:
    st.error("Snowpark session not available. Cannot run audit.")
    st.stop()

user_input = st.chat_input("What is your question?")
if user_input:
    process_analyst_message(session=session, prompt=user_input)

# Center this button
_, btn_container, _ = st.columns([2, 6, 2])
if btn_container.button("Clear Chat History", use_container_width=True):
    reset_session_state()

# --- Sidebar for Configuration ---
with st.sidebar:
    st.header("Snowflake Connection")
    # Get initial session to retrieve environment details
    initial_snowpark_session = get_snowpark_session_sis()
    initial_environment_info = None

    if initial_snowpark_session:
        initial_environment_info = get_snowflake_environment_sis(initial_snowpark_session)

    if initial_environment_info:
        st.text_input("Account Identifier", initial_environment_info.get("account", ""), disabled=True)
        st.text_input("Username", initial_environment_info.get("user", ""), disabled=True)
        
        # Editable fields
        # Provide initial values from environment, allow user to change
        # Ensure keys for st.session_state are unique if you plan to use them
        selected_role = st.text_input("Role to Use", initial_environment_info.get("role", ""), key="selected_role_input")
        selected_warehouse = st.text_input("Warehouse to Use (Optional)", initial_environment_info.get("warehouse", ""), key="selected_warehouse_input")
        selected_database = st.text_input("Database to Audit", initial_environment_info.get("database", ""), key="selected_database_input")
        selected_schema = st.text_input("Schema to Audit", initial_environment_info.get("schema", ""), key="selected_schema_input")
        st.info("Account & Username are from the SiS session. Role, Warehouse, Database, and Schema can be overridden for the audit.")
    else:
        st.error("Could not retrieve initial Snowflake environment information. Audit may not run correctly.")
        st.stop()

    st.header("Audit Options")
    audit_tags_input = st.text_input("Object/Column Tag Keys (comma-separated)", placeholder="e.g., PII_STATUS,DATA_OWNER")
    
    include_access_check = st.checkbox("Perform Access Checks", True)
    include_data_dictionary = st.checkbox("Generate Data Dictionary", True)
    include_workflow_dictionary = st.checkbox("Generate Workflow Dictionary", True)
    include_script_analysis = st.checkbox("Analyze Scripts (Views, Procedures, Tasks)", True)
    # include_service_analysis = st.checkbox("Analyze Service Usage", True) # Placeholder for future
    show_compute_consumption = st.checkbox("Show Compute Consumption")
    if show_compute_consumption:
        lookback_days = st.number_input("Lookback Days for Compute", min_value=1, value=7)

# --- Main Area for Results ---
if st.sidebar.button("Run Audit"):
    session = get_snowpark_session_sis() # Re-get session for audit run
    if not session:
        st.error("Snowpark session not available. Cannot run audit.")
        st.stop()

    # Apply context overrides (Role, Warehouse, Database, Schema)
    try:
        # Get current values from sidebar inputs
        # These are now directly the values from the text_input fields due to Streamlit's rerun behavior
        # No need to re-fetch from initial_environment_info if they are editable
        
        effective_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
        if selected_role and selected_role != effective_role:
            session.use_role(selected_role)
            st.sidebar.success(f"Switched to role: {selected_role}")
            effective_role = selected_role # update for access check

        effective_warehouse = session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0]
        if selected_warehouse and selected_warehouse != (effective_warehouse if effective_warehouse else ""): # Handle initial None
            session.use_warehouse(selected_warehouse)
            st.sidebar.success(f"Switched to warehouse: {selected_warehouse}")
        elif not selected_warehouse and effective_warehouse: # User cleared warehouse
             st.sidebar.warning(f"No warehouse selected. Some operations might be slow or fail if a warehouse is required.")


        effective_database = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        if selected_database and selected_database != (effective_database if effective_database else ""):
            session.use_database(selected_database)
            st.sidebar.success(f"Switched to database: {selected_database}")
            effective_database = selected_database
        
        effective_schema = session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0]
        if selected_schema and selected_schema != (effective_schema if effective_schema else ""):
            session.use_schema(selected_schema)
            st.sidebar.success(f"Switched to schema: {selected_schema}")
            effective_schema = selected_schema

        # Update current context for display or use in queries if needed
        # These are the actual context the audit will run under
        current_audit_context = {
            "role": session.sql("SELECT CURRENT_ROLE()").collect()[0][0],
            "warehouse": session.sql("SELECT CURRENT_WAREHOUSE()").collect()[0][0] or "N/A",
            "database": session.sql("SELECT CURRENT_DATABASE()").collect()[0][0] or "N/A",
            "schema": session.sql("SELECT CURRENT_SCHEMA()").collect()[0][0] or "N/A",
        }
        st.sidebar.markdown("---")
        st.sidebar.markdown("**Effective Audit Context:**")
        for k, v in current_audit_context.items():
            st.sidebar.write(f"**{k.capitalize()}:** {v}")
        st.sidebar.markdown("---")


    except Exception as e:
        st.sidebar.error(f"Error applying new context: {e}")
        st.error(f"Could not apply context overrides: {e}. Audit might run with initial context or fail.")
        # Decide if to stop or continue with potentially wrong context
        # st.stop() # Option to stop if context switch is critical

    st.subheader("Audit in Progress...")
    results = {}
    
    # --- 0. Access Checks ---
    if include_access_check:
        with st.spinner("Performing Access Checks..."):
            st.markdown("## 🛡️ Access Check Summary")
            access_report = {}

            # Information Schema (Tables, Views)
            try:
                session.sql(f"SELECT COUNT(*) FROM {current_audit_context['database']}.INFORMATION_SCHEMA.TABLES LIMIT 1").collect()
                access_report["Information Schema (Tables/Views)"] = "✅ Accessible"
            except Exception as e:
                access_report["Information Schema (Tables/Views)"] = f"❌ Not Accessible or Error: {str(e).splitlines()[0]}"

            # Cortex Analyst (using ANALYZE_TEXT)
            try:
                # Use a simple Cortex function to test general accessibility
                session.sql("SELECT SNOWFLAKE.CORTEX.SUMMARIZE('This is a test.') AS test").collect()
                access_report["Cortex LLM Functions (e.g., SUMMARIZE)"] = "✅ Accessible"
            except Exception as e:
                error_message = str(e).splitlines()[0] # Get the first line of the error
                access_report["Cortex LLM Functions (e.g., SUMMARIZE)"] = f"❌ Not Accessible or Error: {error_message}"
                if "does not exist or not authorized" in error_message:
                    st.sidebar.warning("Cortex function access error: Ensure the SNOWFLAKE.CORTEX schema and its functions are enabled and the role has USAGE privilege.")
                elif "Failed to establish a new connection" in error_message or "Failed to connect to host" in error_message:
                     st.sidebar.warning("Cortex function access error: Network issue or outage connecting to the Cortex service endpoint. Check network policies and Snowflake status.")
            
            # Stages
            try:
                session.sql(f"SHOW STAGES LIMIT 1").collect() # Will show for current_audit_context['database'].current_audit_context['schema']
                access_report["Stages (Listing)"] = "✅ Accessible (SHOW STAGES)"
            except Exception as e:
                access_report["Stages (Listing)"] = f"❌ Not Accessible or Error: {str(e).splitlines()[0]}"
            
            # Streams
            try:
                session.sql(f"SHOW STREAMS LIMIT 1").collect() # Will show for current_audit_context['database'].current_audit_context['schema']
                access_report["Streams (Listing)"] = "✅ Accessible (SHOW STREAMS)"
            except Exception as e:
                access_report["Streams (Listing)"] = f"❌ Not Accessible or Error: {str(e).splitlines()[0]}"
            
            for item, status in access_report.items():
                st.markdown(f"- **{item}:** {status}")
            results['access_check_report'] = access_report
            st.markdown("---")


    # --- 1. Data Dictionary ---
    if include_data_dictionary:
        with st.spinner("Generating Data Dictionary... (This may take a while for many tags/objects)"):
            st.markdown("## 📖 Data Dictionary")
            parsed_tag_keys = [key.strip() for key in audit_tags_input.split(',') if key.strip()]
            dynamic_tag_select_parts = []
            if parsed_tag_keys:
                for key in parsed_tag_keys:
                    # Sanitize tag key for use as a column alias
                    alias_suffix = re.sub(r'[^A-Za-z0-9_]', '_', key).upper()
                    dynamic_tag_select_parts.append(f"SYSTEM$GET_TAG('{key}', tc.TABLE_NAME, 'TABLE') AS TAG_{alias_suffix}_TABLE")
                    dynamic_tag_select_parts.append(f"SYSTEM$GET_TAG('{key}', tc.TABLE_NAME, cc.COLUMN_NAME, 'COLUMN') AS TAG_{alias_suffix}_COLUMN")
            
            dynamic_tag_select_sql = (", " + ", ".join(dynamic_tag_select_parts)) if dynamic_tag_select_parts else ""

            # Corrected table query (removed redundant join to INFORMATION_SCHEMA.TABLES tt)
            # Using effective database/schema from current session context for filtering
            db_filter = f"AND tc.TABLE_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
            schema_filter = f"AND tc.TABLE_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else ""

            tables_query = f"""
            SELECT
                tc.TABLE_CATALOG AS database_name,
                tc.TABLE_SCHEMA AS schema_name,
                tc.TABLE_NAME AS object_name,
                'TABLE' AS object_type,
                tc.COMMENT AS object_description,
                cc.COLUMN_NAME AS column_name,
                cc.DATA_TYPE AS column_data_type,
                cc.IS_NULLABLE AS column_is_nullable,
                cc.COLUMN_DEFAULT AS column_default,
                cc.COMMENT AS column_description,
                tc.TABLE_TYPE,
                tc.LAST_ALTERED AS object_last_altered,
                tc.CREATED AS object_creation_date
                {dynamic_tag_select_sql}
            FROM
                INFORMATION_SCHEMA.TABLES tc
            JOIN
                INFORMATION_SCHEMA.COLUMNS cc ON tc.TABLE_CATALOG = cc.TABLE_CATALOG
                                            AND tc.TABLE_SCHEMA = cc.TABLE_SCHEMA
                                            AND tc.TABLE_NAME = cc.TABLE_NAME
            WHERE
                tc.TABLE_TYPE = 'BASE TABLE'
                {db_filter}
                {schema_filter};
            """

            # For views, similar structure for tags
            dynamic_view_tag_select_parts = []
            if parsed_tag_keys:
                for key in parsed_tag_keys:
                    alias_suffix = re.sub(r'[^A-Za-z0-9_]', '_', key).upper()
                    # For views, object domain is 'VIEW'
                    dynamic_view_tag_select_parts.append(f"SYSTEM$GET_TAG('{key}', tv.TABLE_NAME, 'VIEW') AS TAG_{alias_suffix}_VIEW")
                    dynamic_view_tag_select_parts.append(f"SYSTEM$GET_TAG('{key}', tv.TABLE_NAME, cc.COLUMN_NAME, 'COLUMN') AS TAG_{alias_suffix}_COLUMN")

            dynamic_view_tag_select_sql = (", " + ", ".join(dynamic_view_tag_select_parts)) if dynamic_view_tag_select_parts else ""
            
            db_view_filter = f"AND tv.TABLE_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
            schema_view_filter = f"AND tv.TABLE_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else ""


            views_query = f"""
            SELECT
                tv.TABLE_CATALOG AS database_name,
                tv.TABLE_SCHEMA AS schema_name,
                tv.TABLE_NAME AS object_name,
                'VIEW' AS object_type,
                tv.COMMENT AS object_description,
                cc.COLUMN_NAME AS column_name,
                cc.DATA_TYPE AS column_data_type,
                cc.IS_NULLABLE AS column_is_nullable,
                cc.COLUMN_DEFAULT AS column_default,
                cc.COMMENT AS column_description,
                tv.LAST_ALTERED AS object_last_altered,
                tv.CREATED AS object_creation_date
                {dynamic_view_tag_select_sql}
            FROM
                INFORMATION_SCHEMA.VIEWS tv
            JOIN
                INFORMATION_SCHEMA.COLUMNS cc ON tv.TABLE_CATALOG = cc.TABLE_CATALOG
                                            AND tv.TABLE_SCHEMA = cc.TABLE_SCHEMA
                                            AND tv.TABLE_NAME = cc.TABLE_NAME
            WHERE 1=1
                {db_view_filter}
                {schema_view_filter};
            """
            # Materialized views query (basic, add tags if needed, similar to views)
            # materialized_views_query = f"""..."""


            tables_df = fetch_data(session, tables_query)
            views_df = fetch_data(session, views_query)
            # materialized_views_df = fetch_data(session, materialized_views_query)

            all_objects_df = pd.concat([tables_df, views_df], ignore_index=True)
            
            if not all_objects_df.empty:
                results['data_dictionary'] = all_objects_df
                st.dataframe(results['data_dictionary'])
                st.download_button(
                    label="Download Data Dictionary (CSV)",
                    data=results['data_dictionary'].to_csv(index=False).encode('utf-8'),
                    file_name="data_dictionary.csv",
                    mime='text/csv'
                )
            else:
                st.info("No tables or views found based on the current scope and filters, or an error occurred fetching them.")
            st.markdown("---")

    # --- 2. Workflow Dictionary (Tasks, Streams, Pipes) ---
    if include_workflow_dictionary:
        with st.spinner("Generating Workflow Dictionary..."):
            st.markdown("## 🔁 Workflow Dictionary")
            db_filter_wf = f"WHERE TASK_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
            schema_filter_wf = f"AND TASK_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' and db_filter_wf else (f"WHERE TASK_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else "")


            # Corrected query for TASKS
            tasks_query = f"""
            SELECT
                NAME AS workflow_name,
                'TASK' AS workflow_type,
                DEFINITION AS definition,
                SCHEDULE AS schedule,
                STATE AS state,
                COMMENT AS description,
                CONCAT_WS('.', TASK_CATALOG, TASK_SCHEMA, NAME) as fully_qualified_name
            FROM INFORMATION_SCHEMA.TASKS
            {db_filter_wf} {schema_filter_wf.replace("TASK_", "") if schema_filter_wf else ""} 
            ;""" # Basic filter concatenation

            # Corrected query for STREAMS
            db_filter_stream = f"WHERE STREAM_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
            schema_filter_stream = f"AND STREAM_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' and db_filter_stream else (f"WHERE STREAM_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else "")
            streams_query = f"""
            SELECT
                NAME AS workflow_name,
                'STREAM' AS workflow_type,
                TABLE_NAME AS source_table,
                MODE AS stream_mode,
                STALE_AFTER AS stale_after,
                COMMENT AS description,
                CONCAT_WS('.', STREAM_CATALOG, STREAM_SCHEMA, NAME) as fully_qualified_name
            FROM INFORMATION_SCHEMA.STREAMS
            {db_filter_stream} {schema_filter_stream.replace("STREAM_", "") if schema_filter_stream else ""}
            ;"""

            # Query for PIPES
            db_filter_pipe = f"WHERE PIPE_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
            schema_filter_pipe = f"AND PIPE_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' and db_filter_pipe else (f"WHERE PIPE_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else "")
            pipes_query = f"""
            SELECT
                PIPE_NAME AS workflow_name,
                'PIPE' AS workflow_type,
                DEFINITION AS definition,
                PATTERN AS pattern,
                NOTIFICATION_CHANNEL AS notification_channel,
                COMMENT AS description,
                CONCAT_WS('.', PIPE_CATALOG, PIPE_SCHEMA, PIPE_NAME) as fully_qualified_name
            FROM INFORMATION_SCHEMA.PIPES
            {db_filter_pipe} {schema_filter_pipe.replace("PIPE_", "") if schema_filter_pipe else ""}
            ;"""
            
            tasks_df = fetch_data(session, tasks_query)
            streams_df = fetch_data(session, streams_query)
            pipes_df = fetch_data(session, pipes_query)

            # Add 'Data Sources' and 'Destination Table' columns with placeholder or basic parsing logic if needed
            # For now, just concatenating the dataframes
            workflow_df = pd.concat([tasks_df, streams_df, pipes_df], ignore_index=True)

            if not workflow_df.empty:
                results['workflow_dictionary'] = workflow_df
                st.dataframe(results['workflow_dictionary'])
            else:
                st.info("No tasks, streams, or pipes found in the current scope.")
            st.markdown("---")


    # --- 3. Script Analysis ---
    if include_script_analysis:
        YAML_STAGE_NAME = "CORTEX_ANALYST_YAMLS" # Name of the shared stage
                # --- App Configuration & State ---
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "active_suggestion" not in st.session_state:
            st.session_state.active_suggestion = None
        if "generated_yaml_path" not in st.session_state:
            st.session_state.generated_yaml_path = None

        try:
        # --- NEW: Ensure the named stage exists on app startup ---
            if not ensure_named_stage_exists(session, selected_database, selected_schema, YAML_STAGE_NAME):
                st.error("FATAL: Could not ensure required named stage exists. App cannot function without it.")
                st.stop() # Stop execution if stage cannot be set up

        except Exception as e:
            st.error(f"FATAL: Failed to get active Snowpark session: {e}. App cannot function.")
            st.stop()
        try:
            session.sql("SELECT 1").collect()
            st.write("✅ Basic SQL execution")
        except Exception as e:
            st.write(f"❌ Basic SQL execution failed: {e}")
            access_ok = False
        try:
            try:
                session.sql("SELECT SNOWFLAKE.CORTEX.SENTIMENT('test access to cortex functions')").collect()
                st.write("✅ Cortex SENTIMENT function (general Cortex SQL access)")
            except:
                session.sql("ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_US'")
                session.sql("SELECT SNOWFLAKE.CORTEX.SENTIMENT('test access to cortex functions')").collect()
                st.write("⚠️ CORTEX CROSS REGION enabled ✅ Cortex SENTIMENT function (general Cortex SQL access)")  
        except Exception as e:
            st.write(f"⚠️ Cortex SENTIMENT function failed: {e.message if hasattr(e, 'message') else e}")
            st.caption("Note: This might not affect the private Analyst API if it uses a different access path/permissions.")
        try:
                # Basic check for tables and columns
                session.sql(f"SELECT COUNT(*) FROM \"{selected_database.upper()}\".INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{selected_schema.upper()}' LIMIT 1").collect()
                session.sql(f"SELECT COUNT(*) FROM \"{selected_database.upper()}\".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{selected_schema.upper()}' LIMIT 1").collect()
                st.success(f"Access to INFORMATION_SCHEMA for `{selected_database}`.`{selected_schema}` confirmed.")
                schema_access_confirmed = True
        except Exception as e:
                st.error(f"Failed to access INFORMATION_SCHEMA for `{selected_database}`.`{selected_schema}`: {e}. Please check permissions for the role '{session.get_current_role()}'.")

        if schema_access_confirmed:
            yaml_content_string = None
            with st.spinner(f"Step 2/3: Generating semantic model YAML for `{selected_database}`.`{selected_schema}`..."):
                yaml_content_string = generate_semantic_model_yaml_string(session, selected_database, selected_schema)

            if yaml_content_string:
                staged_file_path = None
                with st.spinner(f"Step 3/3: Uploading YAML to named stage (`@{selected_database}.{selected_schema}.{YAML_STAGE_NAME}`)..."):
                    now_str = dt.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
                    clean_db_name = selected_database.replace(' ','_').replace('.','_').replace('"','') # Clean for filename
                    clean_schema_name = selected_schema.replace(' ','_').replace('.','_').replace('"','') # Clean for filename
                    dynamic_yaml_filename = f"semantic_model_{clean_db_name}_{clean_schema_name}_{now_str}.yaml"

                    staged_file_path = upload_yaml_to_named_stage(session, yaml_content_string, file_name=dynamic_yaml_filename)
                    st.session_state.generated_yaml_path = staged_file_path

            if st.session_state.generated_yaml_path:
                audit_prompt = (
                    f"I am providing a semantic model (located at `{st.session_state.generated_yaml_path}`) that describes the live database schema: "
                    f"`{selected_database}.{selected_schema}`. "
                    "Based on this semantic model, is the data model for "
                    f"`{selected_database}.{selected_schema}` well structured? "
                    "Please provide a comprehensive analysis of its structure, including potential strengths, weaknesses, and areas for improvement regarding normalization, relationships, naming conventions, data type appropriateness, and overall understandability. "
                    "Provide specific examples or references from the semantic model if possible that would relate to the "
                    f"`{selected_database}.{selected_schema}` schema."
                )
                process_analyst_message(session=session, prompt=audit_prompt, semantic_model_path_for_info=st.session_state.generated_yaml_path)
            else:
                st.error("Failed to upload the dynamic semantic model YAML. Cannot proceed with analysis.")
        elif schema_access_confirmed:
            st.error("Failed to generate the dynamic semantic model YAML content (e.g., no objects found or YAML conversion error). Cannot proceed with analysis.")


        st.header("Chat with Cortex Analyst")


        include_script_analysis_expanded = st.checkbox("Expand search to scripts")
        if include_script_analysis_expanded:
            with st.spinner("Analyzing Scripts with Cortex AI... (This can take time and incur costs)"):
                st.markdown("## 🧠 Script Analysis (via Cortex AI)")
                script_analysis_results = []
                
                # Using effective database/schema for filtering
                db_scripts_cond = f"AND TABLE_CATALOG = '{current_audit_context['database']}'" if current_audit_context['database'] != 'N/A' else ""
                schema_scripts_cond = f"AND TABLE_SCHEMA = '{current_audit_context['schema']}'" if current_audit_context['schema'] != 'N/A' else ""

                # Analyze Views
                views_to_analyze_df = fetch_data(session, f"SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE 1=1 {db_scripts_cond} {schema_scripts_cond} LIMIT 10") # Limit for cost/time
                st.caption(f"Analyzing up to 10 views from `{current_audit_context['database']}`.`{current_audit_context['schema']}`...")
                for index, view in views_to_analyze_df.iterrows():
                    with st.expander(f"View: {view['TABLE_NAME']}"):
                        st.code(view['VIEW_DEFINITION'], language='sql')
                        analysis = analyze_sql_with_cortex(session, view['VIEW_DEFINITION'])
                        if "suggestions" in analysis:
                            st.markdown("**Cortex Suggestions:**")
                            st.text(analysis['suggestions'])
                        elif "error" in analysis:
                            st.error(f"Cortex Analysis Error: {analysis['error']}")

                # Analyze Stored Procedures
                procs_to_analyze_df = fetch_data(session, f"SELECT PROCEDURE_NAME, PROCEDURE_DEFINITION, PROCEDURE_LANGUAGE FROM INFORMATION_SCHEMA.PROCEDURES WHERE 1=1 {db_scripts_cond.replace('TABLE_','PROCEDURE_')} {schema_scripts_cond.replace('TABLE_','PROCEDURE_')} LIMIT 5") # Limit
                st.caption(f"Analyzing up to 5 procedures from `{current_audit_context['database']}`.`{current_audit_context['schema']}`...")
                for index, procedure in procs_to_analyze_df.iterrows():
                    with st.expander(f"Procedure: {procedure['PROCEDURE_NAME']} ({procedure['PROCEDURE_LANGUAGE']})"):
                        st.code(procedure['PROCEDURE_DEFINITION'], language=procedure['PROCEDURE_LANGUAGE'].lower())
                        if procedure['PROCEDURE_LANGUAGE'] == 'SQL':
                            analysis = analyze_sql_with_cortex(session, procedure['PROCEDURE_DEFINITION'])
                        elif procedure['PROCEDURE_LANGUAGE'] == 'PYTHON':
                            analysis = analyze_python_with_cortex(session, procedure['PROCEDURE_DEFINITION'])
                        else: # JAVA, SCALA
                            analysis = {"suggestions": f"Automated analysis for {procedure['PROCEDURE_LANGUAGE']} procedures not yet implemented."}
                        
                        if "suggestions" in analysis:
                            st.markdown("**Cortex Suggestions:**")
                            st.text(analysis['suggestions'])
                        elif "error" in analysis:
                            st.error(f"Cortex Analysis Error: {analysis['error']}")
                st.markdown("---")
    
    # --- 4. Compute Consumption ---
    if show_compute_consumption:
        with st.spinner("Fetching Compute Consumption Data..."):
            st.markdown("## 📊 Compute Consumption (Warehouse Metering)")
            # Using WAREHOUSE_METERING_HISTORY for actual credits
            compute_query = f"""
            SELECT
                START_TIME::DATE AS USAGE_DATE,
                WAREHOUSE_NAME,
                SUM(CREDITS_USED_COMPUTE) AS COMPUTE_CREDITS,
                SUM(CREDITS_USED_CLOUD_SERVICES) AS CLOUD_SERVICES_CREDITS,
                SUM(CREDITS_USED) AS TOTAL_CREDITS
            FROM
                SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE
                START_TIME >= DATEADD(day, -{lookback_days}, CURRENT_TIMESTAMP())
            GROUP BY
                USAGE_DATE, WAREHOUSE_NAME
            ORDER BY
                USAGE_DATE DESC, TOTAL_CREDITS DESC;
            """
            compute_df = fetch_data(session, compute_query)
            
            if not compute_df.empty:
                st.dataframe(compute_df)
                
                # Aggregate for charting (total credits per day)
                chart_df = compute_df.groupby('USAGE_DATE')['TOTAL_CREDITS'].sum().reset_index()
                chart_df['USAGE_DATE'] = pd.to_datetime(chart_df['USAGE_DATE']).dt.date

                st.subheader(f"Total Credits Consumed Per Day (Last {lookback_days} Days)")
                bar_chart = alt.Chart(chart_df).mark_bar().encode(
                    x=alt.X('USAGE_DATE:T', title='Date'),
                    y=alt.Y('TOTAL_CREDITS:Q', title='Total Credits Consumed'),
                    tooltip=['USAGE_DATE:T', 'TOTAL_CREDITS:Q']
                ).properties(
                    title=f"Daily Total Credits Over the Last {lookback_days} Days"
                )
                st.altair_chart(bar_chart, use_container_width=True)
            else:
                st.info("No compute consumption data found for the specified period in SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY.")
            st.markdown("---")


    # session.close() # Usually not needed in SiS as Streamlit manages the lifecycle
    st.success("Audit Completed!")
