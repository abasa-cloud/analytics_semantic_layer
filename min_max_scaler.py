# MidMax Scaler transformation to HR_Raw table created in Snowflake
# WH: headless_bi | DB: semantic_layer | SCH: raw_db

import snowflake.snowpark as sp
from snowflake.snowpark.functions import col
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

def model(dbt, session: sp.Session):
    # Load data from the table
    raw_df = session.table("raw_db.hr_raw_")
    
    # Convert to pandas DataFrame
    raw_pd_df = raw_df.to_pandas()
    
    # Identify numeric columns
    numeric_cols = raw_pd_df.select_dtypes(include=['number']).columns
    
    # Apply MinMaxScaler only to numeric columns
    scaler = MinMaxScaler()
    scaled_numeric_df = pd.DataFrame(scaler.fit_transform(raw_pd_df[numeric_cols]), columns=numeric_cols)
    
    # Combine the scaled numeric columns with the non-numeric columns
    non_numeric_cols = raw_pd_df.select_dtypes(exclude=['number']).columns
    final_df = pd.concat([raw_pd_df[non_numeric_cols], scaled_numeric_df], axis=1)
    
    # Convert the final DataFrame back to a Snowpark DataFrame
    final_snow_df = session.create_dataframe(final_df)
    
    return final_snow_df
