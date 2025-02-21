import requests
import os
import pandas as pd
import ast
from airflow.models import Variable
from dags.capstone.python_scripts.snowflake_queries import get_snowpark_session

def load_polygon_main(polygon_key,date_to_ingest='2023-01-09'):

    session = get_snowpark_session('andres')
    my_date = date_to_ingest
    polygon_key = 'Em7xrXc5QX01uQqD29xxTrVZXfrrjC6Q'
    # Define the endpoint
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_to_ingest}?adjusted=true&apiKey="+ polygon_key

    example_data = {
      "T": "TTMI",
      "v": '394280',
      "vw": 16.1078,
      "o": 15.96,
      "c": 16.08,
      "h": 16.335,
      "l": 15.96,
      "t": 1673298000000,
      "n": 5416
    }

    # Make the request
    response = requests.get(url)
    table = 'daily_bars'

    # Obtain the result from the response, which comes as a List of Dictionaries
    if response.status_code == 200:
        # Parse the JSON response
        json_results = response.json()
        number_of_records = json_results['resultsCount']
        if number_of_records != 0:
            results_dict = json_results['results']
        else:
            results_dict = []
        
        # Append the Values from each dictionary in a new list of lists
        daily_results = []
        if results_dict:
            for item in results_dict:
                daily_results.append(list(item.values()))
        else:
            print("No results found in results_dict.")

    else:
        print(f"Response Error: {response.status_code}, {response.text}")

    # Create pandas df with the daily results and re-cast types where necessary
    cols = ['TICKER', 'V', 'VW', 'O', 'C', 'H', 'L', 'TIMESTAMP', 'N']
    df = pd.DataFrame(daily_results, columns=cols)
    df['TIMESTAMP'] = df['TIMESTAMP'].astype('Int64')
    df['N'] = df['N'].astype('Int64')
    
    # Ensure only Clean data is ingested
    df.dropna(subset=['TIMESTAMP'], inplace=True)

    columns = []

    # Prep create table statement with proper column casting
    for (key, value) in example_data.items():
        col_name = key  
        if col_name == 'T':
            columns.append('TICKER VARCHAR')
        elif col_name == 't':
            columns.append('TIMESTAMP BIGINT')
        elif col_name == 'V':
            columns.append('V BIGINT')
        elif col_name == 'N':
            columns.append('N BIGINT')
        elif isinstance(value, str):
            columns.append(f"{col_name} VARCHAR")
        elif isinstance(value, bool):
            columns.append(f"{col_name} BOOLEAN")
        elif isinstance(value, float):
            columns.append(f"{col_name} FLOAT")
        elif isinstance(value, int):
            columns.append(f"{col_name} NUMBER")


    columns_str = ','.join(columns)

    # create_ddl = f'CREATE TABLE IF NOT EXISTS {table} ({columns_str})'
    create_ddl = f'CREATE TABLE if not exists andres.{table} ({columns_str})'

    # Delete records to Prevent duplicates
    del_ddl = f"DELETE FROM {table} WHERE TO_DATE(TO_TIMESTAMP(timestamp / 1000)) = '{my_date}'"

   
    # Run statements and append to DF
    if daily_results != []:
        session.sql(create_ddl).collect()
        session.sql(del_ddl).collect()
        dataframe = session.create_dataframe(df)
        dataframe.write.mode("append").save_as_table(table)
        if len(df)>0:
            print(f"{len(df)} records loaded successfully")
    else:
        print('Data is empty!')
        return 1
