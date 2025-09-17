import os
from dotenv import load_dotenv
from snowflake.connector import connect
import pandas as pd

def query_job_listings(query = 'SELECT * FROM mart_jobs'):
    load_dotenv()
    
    
    with connect(user = os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        role=os.getenv("SNOWFLAKE_ROLE"),    
    ) as conn:
        
        df = pd.read_sql(query, conn)
        return df
    
