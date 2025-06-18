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
import logging
from typing import List, Dict, Optional
from collections import defaultdict

# ------------------------------------------------ APP FUNCTIONS ------------------------------------------------

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

# --- Named Stage Management ---
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

# --- Schema Introspection ---
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

# --- YAML Generation and Upload to stage ---
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

# --- Get Cortex Summary for stateful conversation, LSTM like ---
def summarize_text(text: str) -> str:
    """
    Summarizes the provided text using a simple algorithm.
    This is a placeholder for more complex summarization logic.
    """
    try:
        escaped_prompt = text.replace("'", "") # Escape for SQL string literal
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{escaped_prompt}') AS response_text").collect()

        if result and result[0]['RESPONSE_TEXT']:
                return result[0]['RESPONSE_TEXT']
        else:
                return "Cortex LLM returned an empty response."
    except Exception as e:
        logging.error(f"Exception calling Cortex LLM: {e}")
        st.error(f"Error calling Cortex LLM for analysis: {e}")
        return {"error": f"Error calling Cortex LLM: {e}"}

# --- handle cortex messages accross varying context---
def process_analyst_message(session: Session, prompt: str, semantic_model_path_for_info: str = None, stateful: str = 'False',obj_def: str = None,obj_type: str = None,obj_lang: str = None,obj_name: str = None) -> None:
    """
    Processes a prompt, sends it to the Cortex LLM, and displays the response.
    semantic_model_path_for_info is just for display, not directly used by LLM.
    """
    if not prompt:
        st.error("Prompt cannot be empty.")
        return
    if obj_name is not None: 
        with st.expander(f"**{obj_type}:** {obj_name} ({obj_lang})"):
            st.code(obj_def, language=obj_lang.lower())
            try:
                st.session_state.messages.append(
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
                )
            except Exception as e: 
                st.error(f"error getting access to session {e}")
        
            with st.chat_message("assistant"):
                with st.spinner("Cortex LLM is thinking... This may take a moment for complex analysis."):
                    try:
                        # Call the updated send_message_to_analyst function
                        if stateful == 'False':
                            response = send_message_to_analyst(session=session, prompt_text=prompt)
                        else:
                            response = send_message_to_analyst(session=session, prompt_text= stateful+ prompt)
                        if "error" in response:
                            st.error(f"LLM processing failed: {response['error']}")
                        else:
                            content = response["message"]["content"]
                            display_analyst_content(content=content,obj_name=obj_name)
                            st.session_state.messages.append({"role": "assistant", "content": content})
                    except Exception as e:
                        st.error(f"Failed to get full response from LLM due to an earlier error: {e}")
            
    else:
        try:
            st.session_state.messages.append(
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
            )
        except Exception as e: 
            st.error(f"error getting access to session {e}")
    
        with st.chat_message("assistant"):
            with st.spinner("Cortex LLM is thinking... This may take a moment for complex analysis."):
                try:
                    # Call the updated send_message_to_analyst function
                    if stateful == 'False':
                        response = send_message_to_analyst(session=session, prompt_text=prompt)
                    else:
                        response = send_message_to_analyst(session=session, prompt_text= stateful+ prompt)
                    if "error" in response:
                        st.error(f"LLM processing failed: {response['error']}")
                    else:
                        content = response["message"]["content"]
                        display_analyst_content(content=content)
                        st.session_state.messages.append({"role": "assistant", "content": content})
                except Exception as e:
                    st.error(f"Failed to get full response from LLM due to an earlier error: {e}")

# --- Analyst Interaction Functions ---
def send_message_to_analyst(session: Session, prompt_text: str) -> dict:
    """
    Sends a text prompt to a Snowflake Cortex LLM (e.g., snowflake-arctic)
    to get a text-based response for analysis/suggestions.
    """
    try:

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
    
# --- Display Analyst prompt response ---
def display_analyst_content(content: list, message_index: int = None,obj_name: str = None) -> None:
    effective_message_index = message_index if message_index is not None else len(st.session_state.get("messages", []))
    for item_index, item in enumerate(content):
        if item["type"] == "text":
            response = item["text"]
            st.markdown(response)
            if obj_name is not None: 

                # Add button to create recommended object if a SQL definition is clear
                # This assumes 'analysis['suggestions']' might contain the SQL or you parse it.
                # For a real implementation, you'd want a more robust way to extract the SQL
                # from the LLM's free-form text response. Let's assume it's in an 'item' property
                # in your current `display_analyst_content` if it's type "sql".
                # For a simpler demo, let's make a generic button.
        
                # A placeholder for extracting SQL from the AI's suggestion text
                # This is very naive and would need a robust regex or parsing for production
                recommended_sql_match = re.search(r"```sql\n(.*?)```", response, re.DOTALL)
                if recommended_sql_match:
                    recommended_sql = recommended_sql_match.group(1).strip()
                    st.subheader("Recommended Object Creation:")
                    object_type_option = st.radio(f"Create as for {obj_name}:", ["View", "Table"], key=f"obj_type_{obj_type}")
                    if object_type_option == "Table":
                        replicate_data_option = st.checkbox("Replicate Data (CREATE TABLE AS SELECT)", key=f"replicate_data_{obj_name}")
                    else:
                        replicate_data_option = False # Not applicable for views
        
                    if st.button(f"Create Recommended {object_type_option} for {obj_name}", key=f"create_{object_type_option}_{obj_name}"):
                        create_ai_recommended_object(
                            session,
                            object_type_option,
                            obj_name, # Use original name as base for AI_ prefix
                            recommended_sql,
                            current_audit_context['database'],
                            current_audit_context['schema'],
                            replicate_data_option
                        )
        elif item["type"] == "suggestions": # This type might not be directly returned by COMPLETE, but kept for compatibility
            suggestion_expander_key = f"expander_sugg_{effective_message_index}_{item_index}"
            with st.expander("Suggestions", expanded=True, key=suggestion_expander_key):
                for suggestion_index, suggestion_text in enumerate(item["suggestions"]):
                    # If suggestions are plain text, just display them.
                    # If they were structured (e.g., from former API), you might parse them.
                    st.markdown(f"- {suggestion_text}")
        elif item["type"] == "sql":
            st.markdown('sql')
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

#  --- Confirmation creation / replication statement ---
def confirm_action(action_key: str, message: str) -> bool:
    """
    Displays a confirmation message and returns True if confirmed, False otherwise.
    Uses st.session_state to manage the confirmation flow across reruns.
    """
    # Initialize confirmation state for this specific action
    if f'confirm_{action_key}' not in st.session_state:
        st.session_state[f'confirm_{action_key}'] = False

    # If action is not yet confirmed, show confirmation UI
    if not st.session_state[f'confirm_{action_key}']:
        st.warning(message)
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button("Confirm", key=f'confirm_yes_{action_key}'):
                st.session_state[f'confirm_{action_key}'] = True
                st.experimental_rerun() # Rerun to hide confirmation and proceed
        with col2:
            if st.button("Cancel", key=f'confirm_no_{action_key}'):
                st.session_state[f'confirm_{action_key}'] = False # Explicitly set to false
                st.experimental_rerun() # Rerun to clear warning
        return False # Action not yet confirmed

    # If action was confirmed in a previous rerun, reset state and proceed
    else:
        st.session_state[f'confirm_{action_key}'] = False # Reset for future use
        return True # Action is confirmed
    
# --- AI Recommended Object Creation ---
def create_ai_recommended_object(session: Session, object_type: str, object_name: str, definition_sql: str, target_db: str, target_schema: str, replicate_data: bool = False):
    """
    Creates an AI-recommended SQL object (View or Table) prefixed with 'ai_'.
    If object_type is 'TABLE' and replicate_data is True, it will create a table as SELECT from the definition_sql.
    Otherwise, it creates a view from definition_sql.
    """
    full_object_name = f'"{target_db}"."{target_schema}"."AI_{object_name.upper()}"'
    
    if object_type.upper() == 'TABLE':
        if replicate_data:
            # Create table as select, replicating data
            create_sql = f"CREATE OR REPLACE TABLE {full_object_name} AS {definition_sql}"
            confirmation_message = f"**Confirm Action:** Are you sure you want to create or replace table `{full_object_name}` with data replicated from the recommended SQL? This will incur compute costs."
        else:
            st.error("For AI-recommended TABLES, the 'replicate data' option is typically required to create it 'AS SELECT'. For an empty table based on the structure, specific DDL from AI is needed, or a 'CREATE TABLE ... LIKE' operation which is outside current scope.")
            return False # Exit if not replicating data for tables
    elif object_type.upper() == 'VIEW':
        create_sql = f"CREATE OR REPLACE VIEW {full_object_name} AS {definition_sql}"
        confirmation_message = f"**Confirm Action:** Are you sure you want to create or replace view `{full_object_name}`?"
    else:
        st.error(f"Unsupported object type for creation: {object_type}. Only 'TABLE' and 'VIEW' are supported.")
        return False

    if confirm_action(f"create_ai_object_{object_name}", confirmation_message):
        try:
            st.info(f"Creating {object_type.lower()} `{full_object_name}`...")
            session.sql(create_sql).collect()
            st.success(f"Successfully created {object_type.lower()} `{full_object_name}`.")
            st.code(create_sql, language='sql')
            return True
        except Exception as e:
            st.error(f"Failed to create {object_type.lower()} `{full_object_name}`: {e}")
            return False
    else:
        st.info("Action cancelled by user.")
        return False
    
# --- Schema Cloning on AI Recommendations ---
def clone_and_adjust_schema(session: Session, source_db: str, source_schema: str, recommendation_prompt: str):
    """
    Clones the source schema to a new 'ai_recommendation_' prefixed schema.
    Then, sends a prompt to Cortex AI to get SQL DDL recommendations for adjustments within the cloned schema.
    """
    timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    cloned_schema_name = f'AI_RECOMMENDED_{source_schema.upper()}_{timestamp}'
    full_cloned_schema_path = f'"{source_db}"."{cloned_schema_name}"'

    clone_sql = f'CREATE SCHEMA IF NOT EXISTS {full_cloned_schema_path} CLONE "{source_db}"."{source_schema}" WITH MANAGED ACCESS;'
    
    confirmation_message = f"**Confirm Action:** Are you sure you want to clone schema `{source_db}`.`{source_schema}` to `{full_cloned_schema_path}`? This will create a copy of all tables/views and may incur storage costs."
    
    if confirm_action(f"clone_schema_{source_schema}", confirmation_message):
        try:
            st.info(f"Cloning schema `{source_db}`.`{source_schema}` to `{full_cloned_schema_path}`...")
            session.sql(clone_sql).collect()
            st.success(f"Schema `{full_cloned_schema_path}` cloned successfully.")
            st.code(clone_sql, language='sql')

            st.subheader(f"Requesting AI Recommendations for Cloned Schema: `{full_cloned_schema_path}`")
            st.info("Cortex AI will now analyze your request and provide DDL for adjustments to the cloned schema.")

            # Construct the prompt for the LLM to get DDL recommendations
            llm_prompt = f"""
            I have cloned the schema '{source_db}.{source_schema}' to a new schema called '{full_cloned_schema_path}'.
            Based on the following data model recommendations you previously provided:
            ---
            {recommendation_prompt}
            ---
            
            Please generate **Snowflake SQL DDL statements** to implement these recommendations directly within the cloned schema `{full_cloned_schema_path}`.
            Focus on CREATE, ALTER, DROP, or other DDL to adjust the tables, views, and relationships.
            Prefix all new objects with 'ai_'.
            Ensure all generated SQL statements are fully qualified with `{full_cloned_schema_path}`.
            Present the SQL in a markdown code block. Do NOT include any introductory or concluding text, only the SQL.
            """

            with st.spinner("Asking Cortex AI for DDL recommendations for the cloned schema..."):
                llm_response = send_message_to_analyst(session=session, prompt_text=llm_prompt)
                
                if "error" in llm_response:
                    st.error(f"Failed to get DDL recommendations from Cortex AI: {llm_response['error']}")
                elif llm_response.get("message") and llm_response["message"].get("content"):
                    ai_content = llm_response["message"]["content"][0].get("text", "")
                    st.subheader("Cortex AI Recommended DDL for Cloned Schema:")
                    st.code(ai_content, language='sql')

                    # Option to apply the recommended DDL
                    if ai_content and "CREATE" in ai_content.upper() or "ALTER" in ai_content.upper() or "DROP" in ai_content.upper():
                        if confirm_action(f"apply_ddl_{cloned_schema_name}", f"**Confirm Action:** Are you sure you want to apply the above AI-recommended DDL changes to `{full_cloned_schema_path}`?"):
                            try:
                                # Split SQL statements and execute them
                                # Simple split, might need more robust parsing for complex SQL
                                statements = [s.strip() for s in ai_content.split(';') if s.strip()]
                                for stmt in statements:
                                    if stmt:
                                        session.sql(stmt).collect()
                                st.success(f"Successfully applied AI-recommended DDL to `{full_cloned_schema_path}`.")
                            except Exception as e:
                                st.error(f"Failed to apply AI-recommended DDL to `{full_cloned_schema_path}`: {e}")
                    else:
                        st.info("No executable DDL statements were recommended by Cortex AI, or the response was not in an expected format.")
                else:
                    st.info("Cortex AI did not provide DDL recommendations for the cloned schema.")

            return True
        except Exception as e:
            st.error(f"Failed to clone schema `{source_db}`.`{source_schema}`: {e}")
            return False
    else:
        st.info("Schema cloning cancelled by user.")
        return False
    
# --- Utility Functions ---
def fetch_data(session, query):
    """Executes a SQL query and returns a Pandas DataFrame."""
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error executing query: {query}\n{e}")
        return pd.DataFrame()

# --- analyse text quality ---
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

# --- analyse python quality ---
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

# --- Session multiple element State reset, clear the Ux ---
def reset_session_state():
    """Reset important session state elements."""
    st.session_state.messages = []  # List to store conversation messages
    st.session_state.active_suggestion = None  # Currently selected suggestion
    st.session_state.warnings = []  # List to store warnings
    st.session_state.form_submitted = (
        {}
    )  # Dictionary to store feedback submission for each request

# --- Historic Conversation Display ---
def display_conversation():
    """
    Display the conversation history between the user and the assistant.
    """
    for idx, message in enumerate(st.session_state.messages):
        role = message["role"]
        content = message["content"]
        with st.chat_message(role):
            display_analyst_content(content=content)

# --- Stateful Conversation ( snowflake is inittially stateless) ---
def stateful_conversation():
    text_appended = ''
    if len(st.session_state.messages) == 0:
        text_appended = ". No conversation history available."
    else:
        counter = 0
        for idx, message in enumerate(st.session_state.messages):
            role = message["role"]
            for item_index, item in enumerate(message["content"]):
                if item["type"] == "text":
                    content = item["text"]
            text_appended += '. Message: ' + str(counter)  + ', role: ' + role + ", value: " + content
            counter += 1
            text_appended = summarize_text(text_appended)
        if counter > 20:
            st.warning("Conversation is getting long. Consider clearing history for better performance. summarizing the conversation.")
    return text_appended

# --- Discovery Functions for database objects---
def list_scriptable_objects(
    session: Session, 
    database_name: str, 
    schema_name: str, 
    limit_per_type: Optional[int] = 10
) -> List[Dict]:
    """
    DISCOVERY FUNCTION: Lists all scriptable objects in a schema and their owners.
    """
    discovered_objects = []
    
    # Configuration now includes the 'owner_col' for each object type
    object_configs = [
        {"type": "TABLE", "ddl_name": "TABLE", "info_schema_view": "TABLES", "name_col": "TABLE_NAME", "schema_col": "TABLE_SCHEMA", "owner_col": "TABLE_OWNER"},
        {"type": "VIEW", "ddl_name": "VIEW", "info_schema_view": "VIEWS", "name_col": "TABLE_NAME", "schema_col": "TABLE_SCHEMA", "owner_col": "TABLE_OWNER"},
        {"type": "MATERIALIZED VIEW", "ddl_name": "VIEW", "info_schema_view": "MATERIALIZED_VIEWS", "name_col": "TABLE_NAME", "schema_col": "TABLE_SCHEMA", "owner_col": "TABLE_OWNER"},
        {"type": "FUNCTION", "ddl_name": "FUNCTION", "info_schema_view": "FUNCTIONS", "name_col": "FUNCTION_NAME", "schema_col": "FUNCTION_SCHEMA", "owner_col": "FUNCTION_OWNER", "signature_col": "ARGUMENT_SIGNATURE", "lang_col": "LANGUAGE"},
        {"type": "PROCEDURE", "ddl_name": "PROCEDURE", "info_schema_view": "PROCEDURES", "name_col": "PROCEDURE_NAME", "schema_col": "PROCEDURE_SCHEMA", "owner_col": "PROCEDURE_OWNER", "signature_col": "ARGUMENT_SIGNATURE", "lang_col": "PROCEDURE_LANGUAGE"},
        {"type": "PIPE", "ddl_name": "PIPE", "info_schema_view": "PIPES", "name_col": "PIPE_NAME", "schema_col": "PIPE_SCHEMA", "owner_col": "PIPE_OWNER"},
        {"type": "TASK", "ddl_name": "TASK", "info_schema_view": "TASKS", "name_col": "NAME", "schema_col": "SCHEMA_NAME", "owner_col": "OWNER"},
        {"type": "STREAM", "ddl_name": "STREAM", "info_schema_view": "STREAMS", "name_col": "STREAM_NAME", "schema_col": "STREAM_SCHEMA", "owner_col": "OWNER"},
        {"type": "DYNAMIC TABLE", "ddl_name": "DYNAMIC TABLE", "info_schema_view": "DYNAMIC_TABLES", "name_col": "NAME", "schema_col": "SCHEMA_NAME", "owner_col": "OWNER"},
    ]

    logging.info(f"Starting object discovery for {database_name}.{schema_name}...")
    limit_clause = f"LIMIT {limit_per_type}" if limit_per_type is not None else ""

    for config in object_configs:
        try:
            query_cols = [config['name_col'], config['owner_col']] # Add owner_col to SELECT
            if 'signature_col' in config: query_cols.append(config['signature_col'])
            if 'lang_col' in config: query_cols.append(config['lang_col'])
            
            query = f"SELECT {', '.join(query_cols)} FROM {database_name}.INFORMATION_SCHEMA.{config['info_schema_view']}  {limit_clause}"
            
            rows = session.sql(query).collect()
            if not rows: continue

            for row in rows:
                object_name_val = row[config['name_col']]
                owner = row[config['owner_col']]
                lang_col_name = config.get('lang_col')
                language = row[lang_col_name] if lang_col_name and lang_col_name in row.as_dict() else 'SQL'

                if 'signature_col' in config:
                    full_name_for_ddl = f'"{object_name_val}"{row[config["signature_col"]]}'
                    display_name = f'{object_name_val}{row[config["signature_col"]]}'
                else:
                    full_name_for_ddl = f'"{object_name_val}"'
                    display_name = object_name_val
                
                discovered_objects.append({
                    "display_name": display_name,
                    "full_name_for_ddl": full_name_for_ddl,
                    "object_type_for_ddl": config['ddl_name'],
                    "object_type_display": config['type'],
                    "language": language,
                    "owner": owner, # Store the owner
                })
        except SnowparkSQLException as e:
            logging.warning(f"Could not list {config['type']}s. Skipping. Error: {e.message.strip()}")

    logging.info(f"Discovery finished. Found {len(discovered_objects)} potential objects.")
    return discovered_objects

# --- Scrape database Grants ---
def get_direct_grants(session: Session, object_type: str, fqn: str) -> List[Dict]:
    """
    Gets the direct grants for a specific Snowflake object.
    
    Note: This does not show inherited grants through role hierarchies.
    """
    grants = []
    try:
        # The object type for SHOW GRANTS can sometimes differ from GET_DDL
        # e.g., MATERIALIZED VIEW is just VIEW. We'll handle this simply.
        show_type = "VIEW" if object_type == "MATERIALIZED VIEW" else object_type
        
        # Use single quotes to handle special characters in FQN
        query = f"SHOW GRANTS ON {show_type} '{fqn}'"
        grants_df = session.sql(query).collect()
        for row in grants_df:
            grants.append({
                "privilege": row['privilege'],
                "grantee_name": row['grantee_name'],
                "granted_on": row['granted_on']
            })
    except SnowparkSQLException as e:
        logging.error(f"Could not get grants for {fqn}. Your role may lack privileges. Error: {e.message.strip()}")
    return grants

# --- Scrape DDL for Objects ---
def scrape_object_definitions(
    session: Session, 
    database_name: str, 
    schema_name: str, 
    objects_to_scrape: List[Dict]
) -> List[Dict]:
    """
    EXTRACTION FUNCTION: Scrapes the DDL for a provided list of objects.

    Iterates through a list of pre-discovered objects and uses GET_DDL
    to fetch their creation code.

    Args:
        session: The active Snowpark session object.
        database_name: The database where the objects reside.
        schema_name: The schema where the objects reside.
        objects_to_scrape: The list of objects from list_scriptable_objects().

    Returns:
        A list of dictionaries, now including the 'definition' for each object.
    """
    scraped_definitions = []
    logging.info(f"Starting DDL scraping for {len(objects_to_scrape)} objects...")

    for obj in objects_to_scrape:
        try:
            ddl_query = f"SELECT GET_DDL('{obj['object_type_for_ddl']}', '{database_name}.{schema_name}.{obj['full_name_for_ddl']}') AS DDL"
            ddl_result = session.sql(ddl_query).collect()
            
            if ddl_result and ddl_result[0]['DDL']:
                # Append the full object details along with its definition
                final_object_data = {
                    'object_name': obj['display_name'],
                    'object_type': obj['object_type_display'],
                    'definition': ddl_result[0]['DDL'],
                    'language': obj['language']
                }
                scraped_definitions.append(final_object_data)
                logging.info(f"Successfully scraped DDL for {obj['display_name']}")
            else:
                logging.warning(f"GET_DDL returned empty result for {obj['display_name']}")
        except SnowparkSQLException as e:
            logging.error(f"Could not get DDL for {obj['display_name']}. Error: {e.message.strip()}")
            
    logging.info(f"Finished scraping. Retrieved {len(scraped_definitions)} definitions.")
    return scraped_definitions

# --- Deduce the purpose of an object up to table columns ---
def get_cortex_summary(session: Session, object_definition: str) -> str:
    """
    Uses the application's existing Cortex logic to generate a summary of an object's purpose.

    Args:
        session (Session): The active Snowpark session.
        object_definition (str): The DDL/code of the Snowflake object.

    Returns:
        str: A plain-text summary of the object's purpose.
    """
    try:
        # 1. Create a precise prompt for the LLM for this specific task.
        prompt = f"""
        Analyze the following SQL DDL and describe the business purpose of this object in one or two concise sentences. 
        Focus on what it does, not how it's built.

        DDL:
        ```sql
        {object_definition}
        ```
        """
        
        # 2. Reuse the existing backend function to call the Cortex API.
        response_dict = send_message_to_analyst(session=session, prompt_text=prompt)
        
        # 3. Extract the text content from the response dictionary.
        if "error" not in response_dict:
            # The structure is based on your 'send_message_to_analyst' function's return value
            content_list = response_dict.get("message", {}).get("content", [])
            if content_list and content_list[0].get("type") == "text":
                return content_list[0].get("text", "").strip()
        else:
            logging.error(f"Cortex summary failed: {response_dict['error']}")
            return f"*AI summary could not be generated due to an error: {response_dict['error']}*"

    except Exception as e:
        logging.error(f"An unexpected error occurred in get_cortex_summary: {e}")
        return "*AI summary could not be generated due to an unexpected error.*"
    
    return "*AI summary could not be generated.*"

# --- Generate Technical Documentation from explicit content, scraped code and generated YAML---
def generate_technical_documentation(
    semantic_model_yaml: str, 
    object_definitions: List[Dict], 
    database_name: str, 
    schema_name: str
) -> str:
    """
    Generates a clean technical documentation document in Markdown format.

    This function combines a high-level schema definition (from a YAML semantic model)
    with the detailed DDL code for programmatic objects (views, procedures, etc.)
    to create a single, comprehensive document.

    Args:
        semantic_model_yaml (str): A string containing the YAML definition of the schema.
        object_definitions (List[Dict]): The list of scraped object definitions from 
                                         the scrape_object_definitions() function.
        database_name (str): The name of the database being documented.
        schema_name (str): The name of the schema being documented.

    Returns:
        str: A single string containing the full documentation in Markdown format.
    It should contain the following sections:
        - Document Header with database and schema names, generation date
        - Deduced description of the schema from the YAML
        - relationship between objects and their purpose (like data lienage, dataflow ...)
        - Programmatic objects (views, procedures, etc.) with their DDL code
        - Programmatic objects' purpose summaries using Cortex LLM
        - Warnings for any issues encountered during processing
        - role and privilege information for each object
    """
    doc_parts = []
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    # --- 1. Document Header ---
    doc_parts.append(f"# Technical Documentation: {database_name}.{schema_name}")
    doc_parts.append(f"**Generated on:** {now}\n\n---\n")
    doc_parts.append("## Overview")
    doc_parts.append("This document provides a detailed overview of the data model, schema objects, and programmatic logic contained within the specified Snowflake schema.")

    # --- 2. Process the Data Model from YAML ---
    doc_parts.append("\n## Data Model / Schema Objects")
    try:
        # Parse the YAML string into a Python dictionary
        schema_data = yaml.safe_load(semantic_model_yaml)
        if schema_data and 'entities' in schema_data:
            for entity in schema_data.get('entities', []):
                doc_parts.append(f"\n### {entity.get('type', 'OBJECT')}: `{entity.get('name', 'N/A')}`")
                if entity.get('description'):
                    doc_parts.append(f"> {entity['description']}")
                
                

                doc_parts.append("\n**Columns:**")
                # Create a Markdown table for columns
                col_entity = ''
                doc_parts.append("| Column Name | Datatype | Description |")
                doc_parts.append("|-------------|----------|-------------|")
                for col in entity.get('columns', []):
                    col_entity = col_entity + col.get('name', 'N/A') + ' , '
                    col_name = col.get('name', 'N/A')
                    col_type = col.get('datatype', 'N/A')
                    col_desc = col.get('description', '*No description provided.*')
                    doc_parts.append(f"| `{col_name}` | `{col_type}` | {col_desc} |")
                doc_parts.append("\n")
                description_sql = send_message_to_analyst(session = session, prompt_text= 'Deduce the purpose of the table or view from the columns names ' + col_entity)
                content = description_sql["message"]["content"]
                for item_index, item in enumerate(content):
                    doc_parts.append(item["text"])
                
        else:
            doc_parts.append("_Could not find any table or view entities in the semantic model._")
    except yaml.YAMLError as e:
        doc_parts.append(f"**Warning:** Could not parse the semantic model YAML. Error: {e}")
        doc_parts.append("```yaml\n" + semantic_model_yaml + "\n```")

    # --- 3. Process Programmatic & Workflow Objects ---
    doc_parts.append(f"\n\n---\n## Programmatic & Workflow Objects")
    if not object_definitions:
        doc_parts.append("_No programmatic objects (views, procedures, etc.) were found or accessible._")
    else:
        # Group objects by their type for cleaner organization
        grouped_objects = defaultdict(list)
        for obj in object_definitions:
            grouped_objects[obj['object_type']].append(obj)
        
        for object_type, objects in grouped_objects.items():
            doc_parts.append(f"\n### {object_type}s")
            for obj in objects:
                doc_parts.append(f"\n#### `{obj['object_name']}`")
                doc_parts.append(f"**Language:** {obj.get('language', 'SQL')}")
                doc_parts.append(f"```sql\n{obj['definition']}\n```")

    return "\n".join(doc_parts)

# ------------------------------------------------ STREAMLIT APP ------------------------------------------------
st.set_page_config(layout="wide")
st.title("Snowflake Database Audit Tool ❄️")

try:
    if st.session_state.messages is None:
        # re initialise variable 
        reset_session_state()
except:
    reset_session_state()
    
display_conversation()

# check active session 
session = get_snowpark_session_sis() # Re-get session for audit run
if not session:
    st.error("Snowpark session not available. Cannot run audit.")
    st.stop()

user_input = st.chat_input("What is your question?")
if user_input:
    process_analyst_message(session=session, prompt=  user_input, stateful = stateful_conversation() )
    
# Button to clear chat history
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
    
    include_access_check = st.checkbox("Perform Access Checks")
    include_data_dictionary = st.checkbox("Generate Data Dictionary")
    include_workflow_dictionary = st.checkbox("Generate Workflow Dictionary")
    include_cortex_analysis = st.checkbox("Analyze model")
    include_script_analysis = st.checkbox("Analyze Scripts (Views, Procedures, Tasks)")
    include_tech_docs = st.checkbox("Generate Full Technical Documentation")
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

    # --- 2. Workflow Dictionary (Tasks, Streams, Pipes) ---    ------------------------ work in progress, integrate snowpipe
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
    if include_cortex_analysis:
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


    if include_script_analysis:
        st.markdown("## 🧠 Script Analysis (via Cortex AI)")
        
        # Step 1: DISCOVER all scriptable objects in the schema.
        with st.spinner(f"Discovering objects in `{current_audit_context['database']}`.`{current_audit_context['schema']}`..."):
            discovered_objects = list_scriptable_objects(
                session=session,
                database_name=current_audit_context['database'],
                schema_name=current_audit_context['schema'],
                limit_per_type=10  # A sensible default to control cost and time
            )

        if not discovered_objects:
            st.info("No script objects (Views, Procedures, etc.) were found or are accessible in the specified schema.")
        else:
             # Step 2: EXTRACT the DDL for the objects we found.
            with st.spinner(f"Scraping definitions for {len(discovered_objects)} objects..."):
                all_definitions = scrape_object_definitions(
                    session=session,
                    database_name=current_audit_context['database'],
                    schema_name=current_audit_context['schema'],
                    objects_to_scrape=discovered_objects
                )

            st.success(f"Found and analyzed {len(all_definitions)} script objects.")
            
            # Step 3: PROCESS each definition with Cortex.
            for obj in all_definitions:
                obj_type = obj['object_type']
                obj_name = obj['object_name']
                obj_def = obj['definition']
                obj_lang = obj.get('language', 'SQL')

                    
                analysis_prompt = f"""
                Please perform a detailed analysis of the following Snowflake object.
                Object Name: '{obj_name}'
                Object Type: {obj_type}
                Language: {obj_lang}

                Analyze the code for correctness, performance, style, and adherence to Snowflake best practices.
                Provide a summary of its purpose and a list of actionable suggestions for improvement.
                
                --- CODE DEFINITION ---
                {obj_def}
                --- END CODE DEFINITION ---
                Make sure to add the recommended sql ddl statement as part of your suggestion
                """
        
                process_analyst_message(
                    session=session,
                    prompt=analysis_prompt,
                    obj_type = obj_type,
                    obj_name = obj_name,
                    obj_def = obj_def,
                    obj_lang = obj_lang
                )

        st.markdown("---")
    
    # --- 5. Generate Full Technical Documentation ---
    if include_tech_docs:
        with st.container(border=True):
            st.markdown("## 📄 Full Technical Documentation")
            
            with st.spinner("Generating full technical documentation... This may involve multiple steps."):
                st.write("Step 1/4: Generating semantic model...")
                yaml_content_string = generate_semantic_model_yaml_string(session, current_audit_context['database'], current_audit_context['schema'])
                
                st.write("Step 2/4: Discovering objects and owners...")
                discovered_objects = list_scriptable_objects(
                    session=session,
                    database_name=current_audit_context['database'],
                    schema_name=current_audit_context['schema'],
                    limit_per_type=None
                )
                
                st.write(f"Step 3/4: Scraping definitions, grants, and AI summaries for {len(discovered_objects)} objects...")
                all_definitions = scrape_object_definitions(
                    session=session,
                    database_name=current_audit_context['database'],
                    schema_name=current_audit_context['schema'],
                    objects_to_scrape=discovered_objects
                )

                if yaml_content_string and all_definitions:
                    st.write("Step 4/4: Assembling final documentation...")
                    technical_doc_md = generate_technical_documentation(
                        semantic_model_yaml=yaml_content_string,
                        object_definitions=all_definitions,
                        database_name=current_audit_context['database'],
                        schema_name=current_audit_context['schema']
                    )
                    
                    st.success("Technical documentation generated successfully!")
                    
                    # Display the documentation in an expander
                    with st.expander("View Generated Documentation", expanded=True):
                        st.markdown(technical_doc_md)

                    # Provide a download button
                    st.download_button(
                        label="Download Documentation (Markdown)",
                        data=technical_doc_md,
                        file_name=f"technical_docs_{current_audit_context['database']}_{current_audit_context['schema']}.md",
                        mime="text/markdown",
                    )
                else:
                    st.error("Could not generate full documentation because either the semantic model or object definitions could not be retrieved.")
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