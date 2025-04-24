"""
Top-level script to create and send an overview plot per day



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

import plotting

import logging
logger = logging.getLogger(__name__)


def now():
    return datetime.now(timezone.utc)

def now_ymd():
    tmp = now()
    return (tmp.year, tmp.month, tmp.day)


def make_plot(day):
    """
    Wrapper to create the plot for the daily email
    """

    input_data_dir = os.path.join(secretsettings.data_path, "pvpi")

    daily_plots_dir = os.path.join(secretsettings.data_path, "daily_plots")
    os.makedirs(daily_plots_dir, exist_ok=True)

    input_data_path = data_file_path(day, input_data_dir, ext="csv")
    daily_plot_path = data_file_path(day, daily_plots_dir, ext="pdf")

    if os.path.exists(input_data_path):
        plotting.write_daily_overview_fig(input_data_path, daily_plot_path)
    else:
        logger.warning(f"{input_data_path} does not exist!")


    return daily_plot_path




def data_file_path(day, path_prefix, ext="csv"):
    """
    Provides the paths to data files
    Also makes sure the directory for these files exists

    """
            
    yeardirname = day.strftime('%Y')
        
    dbdir = os.path.join(path_prefix, yeardirname)
    os.makedirs(dbdir, exist_ok=True)

    filename = day.strftime('%Y-%m-%d') + "." + ext
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
                #msg.add_attachment(file_data, maintype="text/csv", subtype="text/csv", filename=filename)
                msg.add_attachment(file_data, maintype="application", subtype="pdf", filename=filename)

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

    attachment_file_path = make_plot(yesterday)


    # Send the email:

    #attachment_file_path = data_file_path(yesterday, os.path.join(secretsettings.data_path, "pvpi"), ext="csv")
    subject = f"[Haus] Bericht {nice_day_string}"
    body = f"Bericht {nice_day_string}"
    attachment_filepaths = [attachment_file_path]

    if os.path.exists(attachment_file_path):
        
        # We send the email
        nb_attempts = 1
        keep_trying = True

        while keep_trying:

            if nb_attempts > 1:
                failure_message = f", attempt {nb_attempts}"
            else:
                failure_message = ""

            try:
                send_mail(subject=subject, body=body+failure_message, attachment_filepaths=attachment_filepaths)
                keep_trying = False
            except Exception as e:
                print(f"Attempt {nb_attempts}:")
                print(e)
                nb_attempts += 1
                time.sleep(3*60)
            
            if nb_attempts > 3:
                print("It doesn't work, stopping for today.")
                keep_trying = False


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