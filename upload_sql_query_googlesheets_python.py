import pandas as pd
import polars as pl
import psycopg2
import gspread
from datetime import datetime
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials


current_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

# redshift creds
db_params = {
    'dbname': 'your_database',
    'host': 'your_host.redshift.amazonaws.com',
    'user': 'your_user',
    'password': 'your_password',
    'port': '5439'
}

# query
sql_query = "select * from table"

# files
folder_path = Path(r"folder path")
json_file = folder_path / "your_credentials.json"
archive_file = folder_path / f"data_export_{current_timestamp}.csv"

# read to df
with psycopg2.connect(**db_params) as connection:
    df = pd.read_sql_query(sql=sql_query, con=connection) 
    df_upload = [df.columns.tolist()] + df.values.tolist()

# write to csv
polars_df = pl.from_pandas(df) 
polars_df.write_csv(archive_file)
print(polars_df)

# google sheets api
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name(filename=json_file, scopes=scope)
client = gspread.authorize(credentials)

workbook_key = "your_workbook_key"
worksheet_name = "your_worksheet_name"

workbook = client.open_by_key(workbook_key)
worksheet = workbook.worksheet(worksheet_name)

# clear and upload
worksheet.clear()
worksheet.update('A1', df_upload)
