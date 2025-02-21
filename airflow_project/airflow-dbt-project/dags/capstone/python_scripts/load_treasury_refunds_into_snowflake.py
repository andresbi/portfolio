import requests
import os
import pandas as pd
from dags.capstone.python_scripts.snowflake_queries import get_snowpark_session

def fetch_refund_data(date_to_ingest):
    session = get_snowpark_session('andres')
    url = f"https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/dts/income_tax_refunds_issued?filter=record_date:eq:{date_to_ingest}"
    print('the url is:',url)
    table = 'irs_refund_data'
    example_data = {
        "record_date": "2024-01-02",
        "tax_refund_type": "Taxes - Business Tax Refunds (Checks)",
        "tax_refund_today_amt": "1742787.95"
    }
    
    response = requests.get(url)
    if response.ok:
        daily_results = response.json()['data']


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
    del_ddl = f"DELETE FROM {table} WHERE record_date = '{date_to_ingest}'"
    record_count = len(daily_results)
    if daily_results != []:
        session.sql(create_ddl).collect()
        print("successfully ran: ",create_ddl)
        session.sql(del_ddl).collect()
        print("successfully ran: ",del_ddl)
        dataframe = session.create_dataframe(daily_results, schema=example_data.keys())
        dataframe.write.mode("append").save_as_table(table)
        print(f"Success: {record_count} records from {date_to_ingest} appended")
    else:
        print('Data is empty!')
        return 1


fetch_refund_data('2024-04-01')