import requests
import os
from dags.capstone.python_scripts.snowflake_queries import get_snowpark_session

def load_treasury_main(date_to_ingest):
    session = get_snowpark_session('andres')
    my_date = date_to_ingest
    # Define the endpoint
    url = f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v2/revenue/rcm?fields=record_date,channel_type_desc,tax_category_desc,net_collections_amt&filter=record_date:eq:{date_to_ingest}"

    print("the url is:",url)

    example_data = {
        "record_date": "2024-01-02",
        "channel_type_desc": "Bank",
        "tax_category_desc": "IRS Tax",
        "net_collections_amt": "1742787.95"
    }

    # Make the request
    response = requests.get(url)
    table = 'fiscal_data'

    if response.status_code == 200:
        # Parse the JSON response
        json_results = response.json()
        daily_results = json_results['data']           
        number_of_records = json_results['meta']['count']

    else:
        print(f"Error: {response.status_code}, {response.text}")

    columns = []
    for (key, value) in example_data.items():
        if key == 'record_date':
            columns.append(key + ' DATE')
        elif type(value) is str:
            columns.append(key + ' VARCHAR')
        elif type(value) is bool:
            columns.append(key + ' BOOLEAN')

    columns_str = ','.join(columns)
    create_ddl = f'CREATE TABLE IF NOT EXISTS {table} ({columns_str})'
    del_ddl = f"DELETE FROM {table} WHERE record_date = '{my_date}'"
    record_count = len(daily_results)
    if daily_results != []:
        session.sql(create_ddl).collect()
        print("successfully ran: ",create_ddl)
        session.sql(del_ddl).collect()
        print("successfully ran: ",del_ddl)
        dataframe = session.create_dataframe(daily_results, schema=example_data.keys())
        dataframe.write.mode("append").save_as_table(table)
        print(f"Success: {record_count} records from {my_date} appended")
    else:
        print('Data is empty!')
        return 1
