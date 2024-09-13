import sqlite3
import pandas as pd


def load_to_db(df_dict,db_name='data.db'):
    """
    Load multiple DataFrames into an SQLite database.
    
    Args:
        df_dict (dict): A dictionary where keys are table names and values are DataFrames.
        db_name (str): The name of the SQLite database file. Defaults to 'data.db'.
    """

    con = sqlite3.connect(db_name)

    for table_name, df in df_dict.items():        
        df.to_sql(table_name, con, if_exists='replace')

    # Commit the changes and close the connection
    con.commit()
    con.close()


def get_results_sql(sql_query,db_name='data.db'):
    """
    Return the results of a specified query

    Args:
    sql_query(str): The actual select statement
    db_name (str): The name of the SQLite database file. Defaults to 'data.db'.
    """
    con = sqlite3.connect(db_name)
    cursor = con.cursor()

    cursor.execute(sql_query)

    results = cursor.fetchall()

    if results:
        for result in results:
            print(result)
    else:
        print('no results from query')
    
    con.close()

