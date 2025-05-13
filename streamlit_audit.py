import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import json
import altair as alt
import numpy as np 

# --- Initialize Snowpark Session from Streamlit in Snowflake ---
def get_snowpark_session_sis():
    try:
        return Session.builder.getOrCreate()
    except Exception as e:
        st.error(f"Error getting Snowpark session in Streamlit in Snowflake: {e}")
        return None

def get_snowflake_environment_sis(session: Session):
    try:
        results = session.sql("SELECT CURRENT_USER(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_ROLE(), CURRENT_ACCOUNT()").collect()[0]
        return {
            "user": results[0],
            "warehouse": results[1],
            "database": results[2],
            "schema": results[3],
            "role": results[4],
            "account": results[5],
        }
    except Exception as e:
        st.error(f"Error getting Snowflake environment info: {e}")
        return None
        
# --- Utility Functions ---
def fetch_data(session, query):
    """Executes a SQL query and returns a Pandas DataFrame."""
    try:
        df = session.sql(query).to_pandas()
        return df
    except Exception as e:
        st.error(f"Error executing query: {e}")
        return pd.DataFrame()

def analyze_text_with_cortex(session, text, prompt_suffix):
    """Analyzes text using Snowflake Cortex Analyst with a dynamic prompt."""
    try:
        full_prompt = f"""Analyze the following text and provide insights based on the Snowflake official documentation regarding {prompt_suffix}:\n\n```text\n{text}\n```\n"""
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.ANALYZE_TEXT('{full_prompt}') AS analysis").collect()
        if result and result[0]['ANALYSIS']:
            return json.loads(result[0]['ANALYSIS'])
        else:
            return {"suggestions": f"No specific insights found regarding {prompt_suffix}."}
    except Exception as e:
        return {"error": f"Error during Cortex analysis for {prompt_suffix}: {e}"}

def analyze_sql_with_cortex(session, sql_query):
    """Analyzes SQL using Snowflake Cortex Analyst for style and organization."""
    try:
        prompt = f"""
        Analyze the following SQL query for organization, writing style, and variable naming.
        Suggest improvements based on SQL best practices.

        ```sql
        {sql_query}
        ```
        """
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.ANALYZE_TEXT('{prompt}') AS analysis").collect()
        if result and result[0]['ANALYSIS']:
            return json.loads(result[0]['ANALYSIS'])
        else:
            return {"suggestions": "No specific style/organization suggestions found."}
    except Exception as e:
        return {"error": f"Error during Cortex analysis for SQL: {e}"}

def analyze_python_with_cortex(session, python_code):
    """Analyzes Python using Snowflake Cortex Analyst for style and organization."""
    try:
        prompt = f"""
        Analyze the following Python code for organization, writing style, and variable naming.
        Suggest improvements based on Python best practices (like PEP 8).

        ```python
        {python_code}
        ```
        """
        result = session.sql(f"SELECT SNOWFLAKE.CORTEX.ANALYZE_TEXT('{prompt}') AS analysis").collect()
        if result and result[0]['ANALYSIS']:
            return json.loads(result[0]['ANALYSIS'])
        else:
            return {"suggestions": "No specific style/organization suggestions found."}
    except Exception as e:
        return {"error": f"Error during Cortex analysis for Python: {e}"}

# --- Streamlit App ---

st.title("Snowflake Database Audit Tool")

# --- Sidebar for Configuration ---
with st.sidebar:
    st.header("Snowflake Connection")
    snowpark_session = get_snowpark_session_sis()
    if snowpark_session:
        environment_info = get_snowflake_environment_sis(snowpark_session)
        if environment_info:
            st.text_input("Account Identifier", environment_info.get("account"), disabled=True)
            st.text_input("Username", environment_info.get("user"), disabled=True)
            st.text_input("Warehouse", environment_info.get("warehouse"), disabled=True)
            st.text_input("Database", environment_info.get("database"), disabled=True)
            st.text_input("Schema", environment_info.get("schema"), disabled=True)
            st.text_input("Role", environment_info.get("role"), disabled=True)
            st.info("Connection details loaded from the active Streamlit in Snowflake session.")
        else:
            st.error("Could not retrieve Snowflake environment information.")
            st.stop()
    else:
        st.error("Failed to get Snowpark session in Streamlit in Snowflake.")
        st.stop()

    st.header("Audit Options")
    include_data_dictionary = st.checkbox("Generate Data Dictionary", True)
    include_workflow_dictionary = st.checkbox("Generate Workflow Dictionary", True)
    include_script_analysis = st.checkbox("Analyze Scripts (Tasks, Views, Procedures)", True)
    include_service_analysis = st.checkbox("Analyze Service Usage", True)
    show_compute_consumption = st.checkbox("Show Compute Consumption")
    if show_compute_consumption:
        lookback_days = st.number_input("Lookback Days", min_value=1, value=7)

# --- Main Area for Results ---
if st.sidebar.button("Run Audit"):
    session = get_snowpark_session_sis()
    if not session:
        st.error("Snowpark session not available.")
        st.stop()

    st.subheader("Audit in Progress...")
    results = {}

    # --- 1. Data Dictionary ---
    if include_data_dictionary:
        with st.spinner("Generating Data Dictionary..."):
            tables_query = """
            SELECT
                tc.TABLE_CATALOG AS database_name,
                tc.TABLE_SCHEMA AS schema_name,
                tc.TABLE_NAME AS table_name,
                'TABLE' AS object_type,
                tt.COMMENT AS table_description,
                cc.COLUMN_NAME AS column_name,
                cc.DATA_TYPE AS column_data_type,
                cc.IS_NULLABLE AS column_is_nullable,
                cc.COLUMN_DEFAULT AS column_default,
                cc.COMMENT AS column_description,
                tc.TABLE_TYPE AS table_type,
                tt.LAST_ALTERED AS last_modified_date,
                tt.CREATED AS creation_date,
                // SYSTEM$GET_TAG(tc.TABLE_NAME, 'TABLE', 'your_tag_name') AS table_tag,
                // SYSTEM$GET_TAG(tc.TABLE_NAME, cc.COLUMN_NAME, 'your_column_tag_name') AS column_tag
            FROM
                INFORMATION_SCHEMA.TABLES tc
            JOIN
                INFORMATION_SCHEMA.TABLES tt ON tc.TABLE_CATALOG = tt.TABLE_CATALOG
                                            AND tc.TABLE_SCHEMA = tt.TABLE_SCHEMA
                                            AND tc.TABLE_NAME = tt.TABLE_NAME
            JOIN
                INFORMATION_SCHEMA.COLUMNS cc ON tc.TABLE_CATALOG = cc.TABLE_CATALOG
                                            AND tc.TABLE_SCHEMA = cc.TABLE_SCHEMA
                                            AND tc.TABLE_NAME = cc.TABLE_NAME
            WHERE
                tc.TABLE_TYPE = 'BASE TABLE';
            """
            views_query = """
                   SELECT
                        tc.TABLE_CATALOG AS database_name,
                        tc.TABLE_SCHEMA AS schema_name,
                        tc.TABLE_NAME AS view_name,
                        'VIEW' AS object_type,
                        tv.COMMENT AS view_description,
                        cc.COLUMN_NAME AS column_name,
                        cc.DATA_TYPE AS column_data_type,
                        cc.IS_NULLABLE AS column_is_nullable,
                        cc.COLUMN_DEFAULT AS column_default,
                        cc.COMMENT AS column_description,
                        tv.LAST_ALTERED AS last_modified_date,
                        tv.CREATED AS creation_date
                        // SYSTEM$GET_TAG(tc.TABLE_NAME, 'VIEW', 'your_tag_name') AS view_tag,
                        // SYSTEM$GET_TAG(tc.TABLE_NAME, cc.COLUMN_NAME, 'your_column_tag_name') AS column_tag
                    FROM
                        INFORMATION_SCHEMA.VIEWS tc
                    JOIN
                        INFORMATION_SCHEMA.VIEWS tv ON tc.TABLE_CATALOG = tv.TABLE_CATALOG
                                                    AND tc.TABLE_SCHEMA = tv.TABLE_SCHEMA
                                                    AND tc.TABLE_NAME = tv.TABLE_NAME
                    JOIN
                        INFORMATION_SCHEMA.COLUMNS cc ON tc.TABLE_CATALOG = cc.TABLE_CATALOG
                                                    AND tc.TABLE_SCHEMA = cc.TABLE_SCHEMA
                                                    AND tc.TABLE_NAME = cc.TABLE_NAME;
            """
            materialized_views_query = """
            SELECT
                TABLE_CATALOG AS database_name,
                TABLE_SCHEMA AS schema_name,
                TABLE_NAME AS name,
                'MATERIALIZED VIEW' AS type,
                COMMENT AS description,
                '' AS volume_name,
                '' AS data_type,
                NULL AS average_value,
                LAST_ALTERED AS last_modified_date,
                CREATED AS creation_date,
               //SYSTEM$GET_TAG(TABLE_NAME, 'MATERIALIZED VIEW', 'your_tag_name') AS tag -- Replace 'your_tag_name' with actual tag name
            FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS
            """

            all_objects_df = pd.concat([
                fetch_data(session, tables_query),
                fetch_data(session, views_query),
               # fetch_data(session, materialized_views_query)
            ])
            
        if not all_objects_df.empty:
            data_dictionary = []
            results['data_dictionary'] = pd.DataFrame(all_objects_df)
            st.subheader("Data Dictionary")
            st.dataframe(results['data_dictionary'])
        else:
            st.info("No tables, views, or materialized views found based on the scope.")

    # --- 2. Workflow Dictionary ---
    if include_workflow_dictionary:
        with st.spinner("Generating Workflow Dictionary..."):
            tasks_query = """
            SELECT
                STAGE_NAME AS NAME,
                STAGE_CATALOG AS DEFINITION,
                COMMENT AS DESCRIPTION
            FROM INFORMATION_SCHEMA.STAGES
            """
            streams_query = """
            SELECT
                PIPE_NAME AS NAME,
                PIPE_CATALOG AS DEFINITION,
                COMMENT AS DESCRIPTION
            FROM INFORMATION_SCHEMA.PIPES
            """

            tasks_df = fetch_data(session, tasks_query)
            streams_df = fetch_data(session, streams_query)

            workflow_dictionary = []

            for index, task in tasks_df.iterrows():
                # Simple parsing of task definition to find potential source and target tables
                definition = task['DEFINITION'].upper()
                sources = []
                destinations = []
                if "SELECT" in definition and "FROM" in definition:
                    start_index = definition.find("FROM") + len("FROM")
                    end_index = definition.find("WHERE") if "WHERE" in definition else len(definition)
                    potential_sources = definition[start_index:end_index].strip().split()
                    sources.extend([src.replace(",", "").strip() for src in potential_sources if "." in src]) # Basic identification

                if "INSERT INTO" in definition:
                    start_index = definition.find("INSERT INTO") + len("INSERT INTO")
                    end_index = definition.find("SELECT") if "SELECT" in definition else len(definition)
                    destination = definition[start_index:end_index].strip().split("(")[0].strip()
                    destinations.append(destination)
                elif "CREATE OR REPLACE TABLE" in definition and "AS SELECT" in definition:
                    start_index = definition.find("CREATE OR REPLACE TABLE") + len("CREATE OR REPLACE TABLE")
                    end_index = definition.find("AS SELECT")
                    destination = definition[start_index:end_index].strip().split("(")[0].strip()
                    destinations.append(destination)

                workflow_dictionary.append({
                    "Workflow Name": task['NAME'],
                    "Description": task['DESCRIPTION'] if pd.notna(task['DESCRIPTION']) else "",
                    "Data Sources": sources,
                    "Destination Table": destinations[0] if destinations else ""
                })

            for index, stream in streams_df.iterrows():
                workflow_dictionary.append({
                    "Workflow Name": stream['NAME'],
                    "Description": stream['DESCRIPTION'] if pd.notna(stream['DESCRIPTION']) else f"Tracks changes on {stream['source_table']}",
                    "Data Sources": [stream['source_table']],
                    "Destination Table": "Stream (Change Data Capture)"
                })

            results['workflow_dictionary'] = pd.DataFrame(workflow_dictionary)
            st.subheader("Workflow Dictionary")
            st.dataframe(results['workflow_dictionary'])

    # --- 3. Script Analysis ---
    if include_script_analysis:
        with st.spinner("Analyzing Scripts..."):
            script_analysis_results = []

          

            # Analyze Views
            views_df = fetch_data(session, "SELECT TABLE_NAME, VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS")
            for index, view in views_df.iterrows():
                analysis = analyze_sql_with_cortex(session, view['VIEW_DEFINITION'])
                if "suggestions" in analysis:
                    for suggestion in analysis['suggestions'].split('\n'):
                        if suggestion.strip():
                            script_analysis_results.append({
                                "Description of Change Needed": suggestion.strip(),
                                "Parent Object": f"View: {view['TABLE_NAME']}",
                                "Gravity (1-5)": "2", # Example gravity
                                "Original Value": view['VIEW_DEFINITION'][:100] + "...",
                                "New Value": "See Cortex Suggestion"
                            })
                elif "error" in analysis:
                    script_analysis_results.append({
                        "Description of Change Needed": f"Cortex Analysis Error: {analysis['error']}",
                        "Parent Object": f"View: {view['TABLE_NAME']}",
                        "Gravity (1-5)": "5",
                        "Original Value": view['VIEW_DEFINITION'][:100] + "...",
                        "New Value": "N/A"
                    })

            # Analyze Stored Procedures (basic retrieval - language dependent analysis might be needed)
            procedures_df = fetch_data(session, "SELECT PROCEDURE_NAME, PROCEDURE_DEFINITION, PROCEDURE_LANGUAGE FROM INFORMATION_SCHEMA.PROCEDURES")
            for index, procedure in procedures_df.iterrows():
                if procedure['PROCEDURE_LANGUAGE'] == 'SQL':
                    analysis = analyze_sql_with_cortex(session, procedure['PROCEDURE_DEFINITION'])
                    if "suggestions" in analysis:
                        for suggestion in analysis['suggestions'].split('\n'):
                            if suggestion.strip():
                                script_analysis_results.append({
                                    "Description of Change Needed": suggestion.strip(),
                                    "Parent Object": f"Procedure: {procedure['PROCEDURE_NAME']}",
                                    "Gravity (1-5)": "3", # Example gravity
                                    "Original Value": procedure['PROCEDURE_DEFINITION'][:100] + "...",
                                    "New Value": "See Cortex Suggestion"
                                })
                    elif "error" in analysis:
                        script_analysis_results.append({
                            "Description of Change Needed": f"Cortex Analysis Error: {analysis['error']}",
                            "Parent Object": f"Procedure: {procedure['PROCEDURE_NAME']}",
                            "Gravity (1-5)": "5",
                            "Original Value": procedure['PROCEDURE_DEFINITION'][:100] + "...",
                            "New Value": "N/A"
                        })
                elif procedure['PROCEDURE_LANGUAGE'] == 'PYTHON':
                    analysis = analyze_python_with_cortex(session, procedure['PROCEDURE_DEFINITION'])
                    if "suggestions" in analysis:
                        for suggestion in analysis['suggestions'].split('\n'):
                            if suggestion.strip():
                                script_analysis_results.append({
                                    "Description of Change Needed": suggestion.strip(),
                                    "Parent Object": f"Procedure (Python): {procedure['PROCEDURE_NAME']}",
                                    "Gravity (1-5)": "3", # Example gravity
                                    "Original Value": procedure['PROCEDURE_DEFINITION'][:100] + "...",
                                    "New Value": "See Cortex Suggestion"
                                })
                  # Analyze Tasks
            tasks_df = fetch_data(session, "SELECT STAGE_NAME, STAGE_CATALOG FROM INFORMATION_SCHEMA.STAGES")
            for index, task in tasks_df.iterrows():
                analysis = analyze_sql_with_cortex(session, task['DEFINITION'])
                if "suggestions" in analysis:
                    for suggestion in analysis['suggestions'].split('\n'):
                        if suggestion.strip():
                            script_analysis_results.append({
                                "Description of Change Needed": suggestion.strip(),
                                "Parent Object": f"Task: {task['STAGE_NAME']}",
                                "Gravity (1-5)": "3", # Example gravity
                                "Original Value": task['STAGE_CATALOG'][:100] + "...",
                                "New Value": "See Cortex Suggestion"
                            })
                elif "error" in analysis:
                    script_analysis_results.append({
                        "Description of Change Needed": f"Cortex Analysis Error: {analysis['error']}",
                        "Parent Object": f"Task: {task['STAGE_NAME']}",
                        "Gravity (1-5)": "5",
                        "Original Value": task['STAGE_CATALOG'][:100] + "...",
                        "New Value": "N/A"
                    })
            if script_analysis_results:
                results['script_analysis'] = pd.DataFrame(script_analysis_results)
                st.subheader("Script Analysis")
                st.dataframe(results['script_analysis'])
            else:
                st.info("No scripts found or no suggestions from analysis.")

    session.close()
    st.success("Audit Completed!")

    # --- Download Buttons ---
    if results.get('data_dictionary') is not None:
        st.download_button(
            label="Download Data Dictionary (CSV)",
            data=results['data_dictionary'].to_csv(index=False).encode('utf-8'),
            file_name="data_dictionary.csv")

    if show_compute_consumption:
        with st.spinner("Fetching Compute Consumption Data..."):
            compute_query = f"""
                SELECT
                    DATE_TRUNC('day', start_time) AS execution_date,
                    SUM(bytes_written_to_result) AS cloud_services_bytes,
                    ROUND(AVG(total_elapsed_time),2 ) AS compute_time
                FROM
                    SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE
                    start_time >= DATEADD(day, -{lookback_days}, CURRENT_TIMESTAMP())
                GROUP BY
                    execution_date
                ORDER BY
                    execution_date;
            """
            compute_df = pd.DataFrame(fetch_data(session, compute_query))
            
            if not compute_df.empty:
                # Convert execution_date to datetime, integerto bytes, decimal to compute time
                compute_df['EXECUTION_DATE'] = pd.to_datetime(compute_df['EXECUTION_DATE'],unit ='D').dt.date
                compute_df['CLOUD_SERVICES_BYTES'] = compute_df['CLOUD_SERVICES_BYTES'].astype('int')
                compute_df['COMPUTE_TIME'] = compute_df['COMPUTE_TIME'].astype('float')
                
                # Handle potential NaN or infinite values
                compute_df = compute_df.dropna(subset=['CLOUD_SERVICES_BYTES', 'COMPUTE_TIME'])
                compute_df = compute_df[pd.notna(compute_df['CLOUD_SERVICES_BYTES'])]
                compute_df = compute_df[pd.notna(compute_df['COMPUTE_TIME'])]
                compute_df = compute_df[np.isfinite(compute_df['CLOUD_SERVICES_BYTES'])]
                compute_df = compute_df[np.isfinite(compute_df['COMPUTE_TIME'])]
                st.info(compute_df)
                st.subheader("Daily Compute Consumption (Bytes)")
    
                chart = alt.Chart(compute_df).mark_bar().encode(
                    x=alt.X('EXECUTION_DATE:T', title='Date'),
                    y=alt.Y('CLOUD_SERVICES_BYTES:Q', title='Bytes Read from Result'),
                    color=alt.Color('COMPUTE_TIME:Q', title='Avg. Compute Time (ms)'),
                    tooltip=['EXECUTION_DATE:T', alt.Tooltip('COMPUTE_TIME:Q', title='Avg. Compute Time (ms)'), 'CLOUD_SERVICES_BYTES:Q']
                ).properties(
                    title=f"Daily Bytes Read and Average Compute Time Over the Last {lookback_days} Days"
                )
                st.altair_chart(chart, use_container_width=True)

            else:
                st.info("No compute consumption data found for the specified period.")
