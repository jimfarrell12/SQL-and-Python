from pathlib import Path
import smtplib
import subprocess
import json
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# config
def load_config(config_path):
    with open(config_path, 'r', encoding='utf-8') as file:
        return json.load(file)


# load config
config_path = Path("path/to/config.json")
config = load_config(config_path)

email_config = config["email"]
base_folder = Path(config["base_folder"])
files = [base_folder / file for file in config["files"]]

current_date = datetime.now().strftime("%m-%d-%Y")
email_subject = f"Automation Report for {current_date}"
results = []

# iterate and capture results
for i, file in enumerate(files, start=1):
    file_name = file.stem  # file name without extension

    try:
        subprocess.run(['python', str(file)], capture_output=True, text=True, check=True)
        results.append(f"{i}.) {file_name}:\nNo Exception.")
    except subprocess.CalledProcessError as e:
        results.append(f"{i}.) {file_name}:\nError: {e.stderr}")

# combine results
email_body = "\n\n".join(results)

# create email
message = MIMEMultipart()
message["From"] = email_config["from"]
message["To"] = ", ".join(email_config["recipients"])
message["Subject"] = email_subject
message.attach(MIMEText(email_body, "plain"))

# send email
try:
    with smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"]) as server:
        server.ehlo()
        server.starttls()
        server.login(email_config["from"], email_config["password"])
        server.sendmail(email_config["from"], email_config["recipients"], message.as_string())
    print("Email sent successfully.")
    
except Exception as e:
    print(f"Failed to send email: {e}")
