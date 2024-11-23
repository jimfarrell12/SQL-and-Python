import pandas as pd
from pathlib import Path
from datetime import datetime

# current date for output file
current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

# files
folder_path = Path("folder/path")
emailed_files = folder_path / "emailed_files"
output_file = folder_path / "comps" / f"{current_date}.xlsx"

# get and sort files
archive_list = list(emailed_files.glob("*.xlsx"))
sorted_files = sorted(archive_list, key=lambda x: x.stat().st_ctime, reverse=True)

# empty df to append for final output
final_df = pd.DataFrame()

# iterate file pairs
for current_file, previous_file in zip(sorted_files, sorted_files[1:]):

    # extract file names
    current_file_name = current_file.stem
    previous_file_name = previous_file.stem

    # read to dfs
    current_df = pd.read_excel(current_file)
    previous_df = pd.read_excel(previous_file)

    # merge dfs
    merged_df = pd.merge(previous_df, current_df, on="orderid", how="outer", indicator=True)

    # filter dfs
    present_in_both = merged_df[merged_df["_merge"] == "both"]
    not_in_current = merged_df[merged_df["_merge"] == "left_only"]
    not_in_previous = merged_df[merged_df["_merge"] == "right_only"]

    # iterate agents
    for agentname in previous_df["agentname"].unique():

        # filter by agentname
        previous_group = previous_df[previous_df["agentname"] == agentname]
        current_group = current_df[current_df["agentname"] == agentname]
        present_in_both_group = present_in_both[present_in_both["agentname_x"] == agentname]
        not_in_current_group = not_in_current[not_in_current["agentname_x"] == agentname]
        not_in_previous_group = not_in_previous[not_in_previous["agentname_y"] == agentname]

        # measure dfs
        previous_volume = len(previous_group)
        current_volume = len(current_group)
        orders_removed = len(not_in_current_group)
        orders_recurring = len(present_in_both_group)
        orders_added = len(not_in_previous_group)
        previous_file_avg_age = previous_group["days_since_expiration"].mean() if not previous_group.empty else 0
        current_file_avg_age = current_group["days_since_expiration"].mean() if not current_group.empty else 0

        # output
        output_data = {
            "current_file_name": [current_file_name],
            "previous_file_name": [previous_file_name],
            "agentname": [agentname],
            "current_volume": [current_volume],
            "previous_volume": [previous_volume],
            "orders_removed": [orders_removed],
            "orders_recurring": [orders_recurring],
            "orders_added": [orders_added],
            "current_avg_age": [current_file_avg_age],
            "previous_avg_age": [previous_file_avg_age],
        }

        # append output to final df
        output_df = pd.DataFrame(output_data)
        final_df = pd.concat([final_df, output_df], ignore_index=True)

# export final df
output_file.parent.mkdir(parents=True, exist_ok=True)  # create folder if not exists
final_df.to_excel(output_file, index=False, sheet_name="Sheet1")
print(final_df)
print("export successful.")
