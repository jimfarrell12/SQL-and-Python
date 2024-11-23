import pandas as pd
import json
import logging
from datetime import datetime
from sqlalchemy import create_engine, text, exc
from pathlib import Path

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# load configuration
def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as file:
        return json.load(file)

# create db uri
def create_db_uri(db_config):
    return f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"

# main process
def main():
    try:
        # load configuration
        config_path = Path("/path/to/config.json")
        config = load_config(config_path)

        # current date
        current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # paths
        folder_path = Path(config["folder_path"])
        sql_file = folder_path / "outliers_postgres.sql"
        archive_file = folder_path / "Archive" / f"{config['pg_table']}_{current_date}.xlsx"

        # read sql file
        if not sql_file.exists():
            logging.error(f"SQL file not found: {sql_file}")
            return

        with open(sql_file, "r", encoding="utf-8") as sql:
            sql_query = text(sql.read())

        # database uris
        redshift_uri = create_db_uri(config["redshift"])
        postgres_uri = create_db_uri(config["postgres"])

        # read data from redshift
        with create_engine(redshift_uri).connect() as redshift_conn:
            df = pd.read_sql(sql_query, con=redshift_conn)
            logging.info("Data fetched successfully from Redshift.")

        # upload data to postgres
        with create_engine(postgres_uri).connect() as postgres_conn:
            try:
                df.to_sql(config["pg_table"], postgres_conn, if_exists="replace", index=False)
                logging.info(f"Data uploaded successfully to PostgreSQL table '{config['pg_table']}'.")
            except exc.SQLAlchemyError as e:
                logging.error(f"Error uploading to PostgreSQL: {e}")
                return

        # save data to an archive file
        archive_file.parent.mkdir(parents=True, exist_ok=True)  # create directory if not exists
        df.to_excel(archive_file, index=False, sheet_name="Sheet1")
        logging.info(f"Data archived successfully at: {archive_file}")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# execute script
if __name__ == "__main__":
    main()
