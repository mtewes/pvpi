"""
Send an overview plot of the last day via email, or upload to server



"""

import sys
import os
import secretsettings
#import plotdb
import requests
import warnings
import urllib3
import json
import time
from datetime import datetime, timedelta, timezone
from babel.dates import format_date, format_datetime, format_time


import schedule

import smtplib
from email.message import EmailMessage


import logging
logger = logging.getLogger(__name__)


def now():
    return datetime.now(timezone.utc)

def now_ymd():
    tmp = now()
    return (tmp.year, tmp.month, tmp.day)


def make_plot():

    """
    yesterday = now() - timedelta(days=1)
        
    dbdirname = self.name
    yeardirname = yesterday.strftime('%Y')
        
    dbdir = os.path.join(self.export_workdir, dbdirname, yeardirname)
    os.makedirs(dbdir, exist_ok=True)
        
    filename = yesterday.strftime('%Y-%m-%d') + ".csv"
    """


def data_file_path(day, path_prefix=None):

    # Date of yesterday for filename:
    #day = now() - timedelta(days=1)
            
    yeardirname = day.strftime('%Y')
        
    dbdir = os.path.join(path_prefix, yeardirname)
    filename = day.strftime('%Y-%m-%d') + ".csv"
    filepath = os.path.join(dbdir, filename)
    return filepath


def send_mail(subject="No Subject", body="Body", attachment_filepaths=None):
    """
    Wrapper around sending an email
    """

    #Set the sender email and password
    from_email_addr = secretsettings.from_email_addr
    from_email_pass = secretsettings.from_email_pass
    
    # Create a message object
    msg = EmailMessage()

    # Set the email body
    msg.set_content(body)

    # Set sender and recipient
    msg['From'] = secretsettings.from_email_addr
    msg['To'] = secretsettings.to_email_addr

    # Set your email subject
    msg['Subject'] = subject

    # Add the attachments
    if attachment_filepaths:
        for file in attachment_filepaths:
            filename = os.path.basename(file)
            with open(file, 'rb') as fp:
                file_data = fp.read()
                msg.add_attachment(file_data, maintype="text/csv", subtype="text/csv", filename=filename)

    # Connecting to server and sending email
    # Edit the following line with your provider's SMTP server details
    server = smtplib.SMTP(*secretsettings.email_server)

    # Comment out the next line if your email provider doesn't use TLS
    server.starttls()
    # Login to the SMTP server
    server.login(from_email_addr, from_email_pass)

    # Send the message
    server.send_message(msg)

    print('Email sent')

    #Disconnect from the Server
    server.quit()



def job():

    yesterday = now() - timedelta(days=1)

    #nice_day_string = yesterday.strftime("%A %Y-%m-%d")
    
    nice_day_string = format_date(yesterday, format='full', locale='DE_de')
    print(f"Starting job for {nice_day_string}...")

    # Create the plot:


    # Send the email:

    attachment_file_path = data_file_path(yesterday, path_prefix=os.path.join(secretsettings.data_path, "pvpi"))
    subject = f"[Haus] Bericht {nice_day_string}"
    body = f"Bericht {nice_day_string}"
    attachment_filepaths = [attachment_file_path]

    if os.path.exists(attachment_file_path):
        send_mail(subject=subject, body=body, attachment_filepaths=attachment_filepaths)

    else:
        logger.warning(f"{attachment_file_path} does not exist!")



def run():


    schedule.every().day.at("03:00").do(job)

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("Bye!")


def main():
    run()
    return 0

if __name__ == '__main__':
    #logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())