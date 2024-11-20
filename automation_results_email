from pathlib import Path
import smtplib
import subprocess
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

current_date = datetime.now().strftime("%m-%d-%Y")

# email info
email_from = "email@mail.com"
email_password = "password"
email_recipients = [email_from]
email_subject = f"Automation Report for {current_date}"

# files
source_folder = Path(r"C:\filepath")
files = [
    source_folder / "Test" / "test_email.py",
    source_folder / "Test" / "asdf.py",  # meant to fail
    source_folder / "Emails" / "daily_email.py",
]

results = []

# iterate and capture results
for i, file in enumerate(files, start=1):
    # file name without extension
    file_name = file.stem  
    
    try:
        subprocess.run(['python', str(file)], capture_output=True, text=True, check=True)
        results.append(f"{i}.) {file_name}:\nNo Exception.")

    except subprocess.CalledProcessError as e:
        results.append(f"{i}.) {file_name}:\nError: {e.stderr}")

# combine results
email_body = "\n\n".join(results)

# create email
message = MIMEMultipart()
message["From"] = email_from
message["To"] = ", ".join(email_recipients)
message["Subject"] = email_subject
message.attach(MIMEText(email_body, "plain"))

# connect and send
with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
    server.ehlo()
    server.starttls()
    server.login(email_from, email_password)
    server.sendmail(email_from, email_recipients, message.as_string())
