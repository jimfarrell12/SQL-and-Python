from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import polars as pl
import psycopg2
import concurrent.futures
import json
import logging

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# load configuration
def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# execute segments
def execute_sql(start_date, end_date, sql_query, db_params):
    try:
        with psycopg2.connect(**db_params) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, (start_date, end_date))
                result = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
                return result
    except Exception as e:
        logging.error(f"Error executing SQL for {start_date} to {end_date}: {e}")
        raise

# main exec
try:
    # load config
    config_path = Path("/path/to/config.json")
    config = load_config(config_path)

    # setup paths and parameters
    folder_path = Path(config["folder_path"])
    sql_file = folder_path / "Queries" / "query.sql"
    archive_path = folder_path / "Archive"
    excel_file = archive_path / f"{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

    db_params = config["db_params"]
    num_segments = config["num_segments"]
    days_prior = config["days_prior"]

    # calculate date ranges
    current_date_time = datetime.now()
    end_date = current_date_time
    start_date = end_date - timedelta(days=days_prior)
    segment_length = timedelta(days=days_prior / num_segments)

    # start logging
    logging.info(f"Process started at {current_date_time}")

    # read sql query
    if not sql_file.exists():
        logging.error(f"SQL file not found: {sql_file}")
        return

    with open(sql_file, 'r', encoding='utf-8') as sql:
        sql_query = sql.read()

    futures = []
    results = []

    # execute queries concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_segments) as executor:
        for i in range(num_segments):
            segment_start = start_date + i * segment_length
            segment_end = start_date + (i + 1) * segment_length
            future = executor.submit(execute_sql, segment_start, segment_end, sql_query, db_params)
            futures.append(future)

        # track progress
        for completed, future in enumerate(concurrent.futures.as_completed(futures), 1):
            logging.info(f"Segment {completed}/{num_segments} completed.")
            results.append(future.result())

    # combine results
    combined_df = pd.concat(results, ignore_index=True)

    # ensure output folder exists
    archive_path.mkdir(parents=True, exist_ok=True)

    # save to file
    pl.from_pandas(combined_df).write_csv(excel_file)
    logging.info(f"Data exported to: {excel_file}")

    # log completion time
    finish_time = datetime.now()
    execution_time = finish_time - current_date_time
    logging.info(f"Process finished at {finish_time}, Total time: {execution_time}")

except Exception as e:
    logging.error(f"Unexpected error: {e}")
