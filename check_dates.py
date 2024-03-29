import duckdb
import os
from datetime import datetime, date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import smtplib

db_file = "my_db.duckdb"
mail_data = json.loads(open(os.path.join(os.path.dirname(__file__), "mail_login.json")).read())


def main():
    with duckdb.connect(os.path.join(os.path.dirname(__file__), db_file)) as conn:
        today = date.today()
        results = conn.execute("select * from entries where date <= $date", {"date": today}).df()
        if len(results) > 0:
            results["date"] = results["date"].dt.strftime("%d.%m.%Y")
            send_email(results.values.tolist(), results["email"].loc[0])


def send_email(db_entries, to_mail: str):
    # Create a multipart message
    msg = MIMEMultipart()
    msg["From"] = mail_data["gmail_name"]
    msg["To"] = to_mail
    if "always_send_to" in mail_data:
        msg["Cc"] = mail_data["always_send_to"]
    msg["Subject"] = f"REMINDER !! Christoph !!"

    text = []
    for i in db_entries:
        text.append(f"{i[1]}  {i[2]}  {i[4]}")
    # Attach the message body
    msg.attach(MIMEText(f"Moin moin,\n\nHier mal ne kleine Erinnerung an:\n\n{"\n".join(text)}", "plain"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(mail_data["gmail_name"], mail_data["gmail_password"])
        if "always_send_to" in mail_data:
            smtp.sendmail(mail_data["gmail_name"], [to_mail, mail_data["always_send_to"]], msg.as_string())
        else:
            smtp.sendmail(mail_data["gmail_name"], to_mail, msg.as_string())


if __name__ == "__main__":
    main()
