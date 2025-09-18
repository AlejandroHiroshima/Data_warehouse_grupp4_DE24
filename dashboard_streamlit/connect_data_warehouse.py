import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from snowflake.connector import connect

@st.cache_resource
def get_connection():
    load_dotenv()
    conn = connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
    return conn

@st.cache_data(show_spinner="Laddar KPI'er...")
def query_job_listings(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    return df
 
