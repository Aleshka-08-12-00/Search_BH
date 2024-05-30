import pandas as pd
import psycopg2
import os


database_config = {
    'host': '172.16.0.86',
    'database': 'api',
    'user': 'api',
    'password': 'qazwsx12!@D',
    'port': '5432',
}

# Connection to the PostgreSQL database
connection = psycopg2.connect(**database_config)

# Fetch data from the PostgreSQL table into a DataFrame
query = "SELECT * FROM a_block_element_search WHERE name NOT LIKE '%Образец%' AND name NOT LIKE '%РЕКЛАМА%';"
df = pd.read_sql_query(query, connection)

# Save df to the file
df.to_csv('my_data_file.csv', index=False)

print("updated")
