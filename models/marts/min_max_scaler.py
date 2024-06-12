from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
import json
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, LabelEncoder

# Function to read credentials from JSON file
def read_creds_from_json():
    try:
        with open('cred.json') as f:
            connection_parameters = json.load(f)
    except Exception as e:
        raise ValueError('Error reading JSON file.') from e
    return connection_parameters

# Function to create a Snowpark connection
def create_snowpark_connection():
    creds = read_creds_from_json()
    return Session.builder.configs(creds).create()

def model(dbt, session):
    # Specify the schema and table names
    schema = "dbt_abarrantes_hkkd"
    table = "hr_raw_"
    
    # Read data from the hr_raw table into a Snowpark DataFrame
    df = session.table(f"{schema}.{table}")

    # Convert the Snowpark DataFrame to a Pandas DataFrame
    pdf = df.to_pandas()

    # Identify non-numeric columns
    non_numeric_columns = pdf.select_dtypes(exclude=['float64', 'int64']).columns

    # Apply Label Encoding to non-numeric columns
    for col in non_numeric_columns:
        le = LabelEncoder()
        pdf[col] = le.fit_transform(pdf[col])

    # Apply Min-Max Scaler to all columns
    scaler = MinMaxScaler()
    pdf = pd.DataFrame(scaler.fit_transform(pdf), columns=pdf.columns)

    # Create a new Snowpark DataFrame from the transformed Pandas DataFrame
    transformed_df = session.create_dataframe(pdf)

    # Return the transformed DataFrame
    return transformed_df