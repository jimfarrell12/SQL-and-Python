import pandas as pd
import psycopg2
import json
import logging
from datetime import datetime
from io import StringIO
from pathlib import Path

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# load config
def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)

# upload data to postgres
def upload_data(df, connection, schema, table):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False, sep="~")
    buffer.seek(0)

    with connection.cursor() as cursor:
        try:
            copy_sql = f"COPY {schema}.{table} FROM STDIN WITH CSV DELIMITER '~'"
            cursor.copy_expert(copy_sql, buffer)
            connection.commit()
            logging.info(f"Data successfully uploaded to {schema}.{table}.")
        except Exception as error:
            connection.rollback()
            logging.error(f"Failed to upload data: {error}")
            raise

# main execution
try:
    # paths
    config_path = Path("/path/to/config.json")
    config = load_config(config_path)

    # postgres connection params
    postgres_params = config["postgres"]

    # read data
    data_file_path = Path(config["data_file"])
    if not data_file_path.exists():
        logging.error(f"Data file not found: {data_file_path}")
        return

    df = pd.read_csv(data_file_path)

    # upload to postgres
    with psycopg2.connect(**postgres_params) as postgres_conn:
        upload_data(df, postgres_conn, config["schema"], config["table"])
    
    logging.info("Process completed successfully.")

except Exception as e:
    logging.error(f"Unexpected error: {e}")
