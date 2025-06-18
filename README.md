# ❄️ Snowflake Database Audit Tool

This Streamlit in Snowflake (SiS) application provides a comprehensive tool for auditing your Snowflake database environment. It leverages Snowflake's `INFORMATION_SCHEMA`, Account Usage views, and Cortex AI functions to generate data dictionaries, analyze workflows, provide script suggestions, and offer AI-driven data model recommendations, including options to implement these changes directly in your Snowflake account.

## ✨ Features

* **Snowflake Context Awareness:** Automatically detects your current Snowflake role, warehouse, database, and schema. Allows overriding these for specific audit contexts.
* **Dynamic Data Dictionary:** Generates a detailed data dictionary for tables, views, and materialized views within a specified database and schema. Includes object and column descriptions, data types, and custom tag values.
* **Workflow Dictionary:** Documents tasks, streams, and pipes, providing insights into automated data pipelines.
* **Script Analysis with Cortex AI:** Analyzes SQL (from views, procedures, tasks) and Python (from procedures) code using Snowflake Cortex AI (`SNOWFLAKE.CORTEX.COMPLETE` with `snowflake-arctic`) for best practices and improvement suggestions.
* **Compute Consumption Monitoring:** Visualizes warehouse credit consumption over a configurable lookback period.
* **AI-Driven Data Model Audit & Suggestions:** Utilizes Snowflake Cortex AI to analyze your database schema's structure and provide comprehensive recommendations on normalization, relationships, naming conventions, and data type appropriateness.
* **Actionable Recommendations (AI-Powered DDL Generation):**
    * **Create AI-Recommended Objects:** Generate and execute `CREATE OR REPLACE VIEW` or `CREATE OR REPLACE TABLE AS SELECT` statements for views/tables suggested by Cortex AI, automatically prefixed with `AI_`.
    * **Clone Schema & Adjust with AI DDL:** Clone an entire schema to a new `AI_RECOMMENDED_` prefixed schema and then ask Cortex AI to generate and apply DDL (Data Definition Language) to this cloned schema based on its data model recommendations.
* **Interactive Chat with Cortex AI:** Engage in a free-form chat with a Snowflake Cortex LLM model (`snowflake-arctic`) for general inquiries related to your Snowflake data.
* **User Confirmation:** Critical database modification actions include a user confirmation step to prevent accidental changes.

## 🚀 How to Deploy and Run

This application is designed to run directly within **Streamlit in Snowflake (SiS)**.

1.  **Access Snowsight:** Log in to your Snowflake account via Snowsight.
2.  **Navigate to Streamlit:** In the left-hand navigation bar, go to **Apps** -> **Streamlit**.
3.  **Create New Streamlit App:** Click the **`+ Streamlit App`** button.
4.  **Configure App:**
    * **Name:** Give your app a descriptive name (e.g., `Database_Audit_Tool`).
    * **Warehouse:** Select a virtual warehouse to run the app (e.g., `XS` or `S` for development).
    * **Database & Schema:** Choose the database and schema where you want to deploy the app. This will also be the default location for the dynamically created named stage (`CORTEX_LLM_YAMLS`) and potentially the cloned schemas if using that feature.
5.  **Upload Code:** Copy the entire Python code from this repository into the provided editor or upload the `.py` file.
6.  **Deploy:** Click **`Deploy`**.

## 🔑 Permissions Required

The Snowflake role executing this Streamlit app will need the following minimum privileges for full functionality:

* **Basic Execution:** `USAGE` on the database and schema where the app is deployed, and `USAGE` on the chosen virtual warehouse.
* **Information Schema Access:** `SELECT` on `SNOWFLAKE.INFORMATION_SCHEMA` views (e.g., `TABLES`, `VIEWS`, `MATERIALIZED_VIEWS`, `COLUMNS`, `TASKS`, `STREAMS`, `PIPES`).
* **Account Usage Access (for Compute Consumption):** `SELECT` on `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY`.
* **Cortex AI Functions:** `USAGE` on the `SNOWFLAKE.CORTEX` schema and `USAGE` on specific Cortex functions like `COMPLETE`, `SUMMARIZE`.
* **Stage Management (for AI YAMLs):**
    * `CREATE SCHEMA` on the database where the app is deployed (if the `CORTEX_LLM_YAMLS` schema needs to be created).
    * `CREATE STAGE` on the schema where the app is deployed (for `CORTEX_LLM_YAMLS`).
    * `READ` on `STAGE` `CORTEX_LLM_YAMLS` for `PUBLIC`.
    * `WRITE` on `STAGE` `CORTEX_LLM_YAMLS` for the app's role.
* **Database Object Creation (for AI Recommendations):**
    * `CREATE VIEW` on the target schema (for AI-recommended views).
    * `CREATE TABLE` on the target schema (for AI-recommended tables).
    * `CREATE SCHEMA` on the target database (for cloning schemas).
    * `SELECT` on the source schema's tables/views (for cloning schemas and `CREATE TABLE AS SELECT`).
    * Appropriate `CREATE`/`ALTER`/`DROP` privileges within the *newly cloned* schema if applying AI-generated DDL adjustments to it.

