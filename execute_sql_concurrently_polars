from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import polars as pl
import psycopg2
import glob
import concurrent.futures

current_date_time = datetime.now()

# start time
start_time = current_date_time
print(f"\nStarted: {start_time}\n")

# files
folder_path = Path(r"/path/to/automation/folder")
sql_file = folder_path / "Queries" / "query.sql"
excel_file = folder_path / "Archive" / f"{current_date_time}.csv"

# database credentials
db_params = {
    'dbname': 'your_dbname',
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'port': 'your_port'
}

# read sql
with open(sql_file, 'r', encoding='utf-8') as sql:
    sql_query = sql.read()

# read sql to df by segment
def execute_sql(start_date, end_date, sql_query, connection):
    with connection.cursor() as cursor:
        cursor.execute(sql_query, (start_date, end_date, start_date, end_date))
        result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
        return result

# date segments
days_prior = 365
end_date = current_date_time
start_date = end_date - timedelta(days=days_prior)
num_segments = 15
segment_length = timedelta(days=days_prior / num_segments)

# concurrently execute
with concurrent.futures.ThreadPoolExecutor(max_workers=num_segments) as executor:
    futures = []

    for i in range(num_segments):
        segment_start = start_date + i * segment_length
        segment_end = start_date + (i + 1) * segment_length
        connection = psycopg2.connect(**db_params)
        future = executor.submit(execute_sql, segment_start, segment_end, sql_query, connection)
        futures.append(future)

# track progress
completed_segments = 0
for future in concurrent.futures.as_completed(futures):
    completed_segments += 1
    print(f"{completed_segments}/{num_segments}")

# list results
results = [] 
for future in futures:
    result = future.result()
    results.append(result)
    combined_df = pd.concat(results, ignore_index=True)

# convert to polars for export
combined_df = pl.from_pandas(combined_df) 
combined_df.write_csv(excel_file)

# print run time
finish_time = datetime.now()
execution_time = finish_time - start_time
print(f"\nFinished: {finish_time}")
print(f"Total time: {execution_time}\n")
