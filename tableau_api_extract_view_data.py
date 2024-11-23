import tableauserverclient as TSC
from datetime import datetime
from pathlib import Path

# output file
current_time = datetime.now().strftime("%m-%d-%Y @%I.%M.%S%p")
folder_path = Path(r"C:/filepath")
file_path = folder_path / f"{current_time}.csv"

# server
server = TSC.Server(
    server_address="https://server.address.com",
    use_server_version=True,
)

# auth
token_auth = TSC.PersonalAccessTokenAuth(
    token_name="token",
    personal_access_token="personal_token",
    site_id="Internal",
)

# targets
target_workbook = "target_workbook"
target_view = "target_view"

# sign in
with server.auth.sign_in(token_auth):

    # pagination offset
    page_number = 1
    page_size = 100

    workbooks = []

    # capture all workbooks
    while True:
        req_options = TSC.RequestOptions(pagesize=page_size, pagenumber=page_number)
        current_workbooks, _ = server.workbooks.get(req_options) #pagination_item
        workbooks.extend(current_workbooks)

        if len(current_workbooks) < page_size:
            break
        page_number += 1

    # match target wb
    matched_workbook = next((wb for wb in workbooks if wb.name == target_workbook), None)

    # capture all views
    if matched_workbook is None:
        print(f"{target_workbook} not found.")
    else:
        server.workbooks.populate_views(matched_workbook)
        all_views = matched_workbook.views

        # match target view
        matched_view = next((view for view in all_views if view.name == target_view), None)

        # export to csv
        if matched_view is None:
            print(f"{target_view} not found.")
        else:
            server.views.populate_csv(matched_view)
            with open(file_path, "wb") as file:
                file.write(b"".join(matched_view.csv))
            print(f"{matched_view.name} data exported.")
