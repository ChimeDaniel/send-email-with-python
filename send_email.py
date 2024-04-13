import mysql.connector
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import requests
import json
from datetime import datetime
from time import gmtime, strftime
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from config import password, email # create config.py and add your email and password in it. I won't be adding config to this repo :)


# define destination table details (abstracted)
dataset = '[dataset_name]'
project_id = '[project_name]'

# load credentials 
path = os.getcwd()
os.chdir(path)
os.system('cd {}'.format(path))
os.system('pwd')
credentials = service_account.Credentials.from_service_account_file(
   '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]' # like ./buypower-mobile-app-4567g0whk234.json for example (not a real keyfile name)
print("Credentials Loaded")

# define GBQ client
client =  bigquery.Client(project = project_id)

# get current date
date =  strftime("%Y_%m_%d", gmtime())

# define function to pull data from GBQ (destination)
def pullDataFromBQ(query):
   project_id = '[project_name]'
   df = pd.read_gbq(query, project_id=project_id)
   return df

# define function to execute queries on GBQ
def bq_execute_query(query) -> object:
    '''
    This function is responsible for executing queries on Bigquery.
    
    It is used to run any query on Bigquery related to the data migration process.
    
    It takes the query and returns the results if the query ran successfully or prints the error message if the process didn't run successfully. 
    
    See example below:

    bq_execute_query("SELECT max(created_at) FROM bpcs.Msgs") -> '2023-01-01' datatype: object
    '''
    print(query)
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    # Start the query, passing in the extra configuration.
    try:
        query_job = client.query(query, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.
        results = query_job.result()
        return results
    except Exception as e:
        print("Failed to run the query {}".format(query))
        print(e)

# get the data from GBQ and export them into csv files
def get_data_from_bq():
    print(date)
    q = f'''
    with api_trans as (
    select distinct api_id, ref,
    string_agg(type,"|" order by id ) type_agg,
    string_agg(distinct token_status,"_") token_status_agg
    from
    (select distinct awt.api_id,awt.ref,awt.type,awt.id,
    if(tvr.vend_request_id is null, 'FAILED','SUCCESS') token_status,
    FROM
      `[dataset].[table_one_name]` awt
    LEFT JOIN
      `[dataset].[table_two_name]` pt
    ON
      awt.ref = pt.order_id
      AND awt.api_id= pt.api_user_id
    LEFT JOIN
      `[dataset].[table_three_name]` vr
    ON
      pt.id = vr.order_id
    LEFT JOIN
      `[dataset].[table_four_name]` tvr
    ON
      vr.id = tvr.vend_request_id
    JOIN
      `[dataset].[table_five_name]` au
    ON
      awt.api_id = au.id
      AND UPPER(au.type)='PREFUND'
    where awt.created_at >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS TIMESTAMP)
    order by awt.id)
    group by 1,2
    )
    -- FIRST SHEET -> Commission not given
    select * from api_trans where type_agg ='vend' and token_status_agg = "SUCCESS"
    '''
    data = bq_execute_query(q)
    first_data = data.to_dataframe()
    print(first_data.info())
    first_data.to_csv(f"api_successful_vend_without_commission_{date}.csv", index = False)

    q = f'''
    with api_trans as (
    select distinct api_id, ref,
    string_agg(type,"|" order by id ) type_agg,
    string_agg(distinct token_status,"_") token_status_agg
    from
    (select distinct awt.api_id,awt.ref,awt.type,awt.id,
    if(tvr.vend_request_id is null, 'FAILED','SUCCESS') token_status,
    FROM
      `[dataset].[table_one_name]` awt
    LEFT JOIN
      `[dataset].[table_two_name]` pt
    ON
      awt.ref = pt.order_id
      AND awt.api_id= pt.api_user_id
    LEFT JOIN
      `[dataset].[table_three_name]` vr
    ON
      pt.id = vr.order_id
    LEFT JOIN
      `[dataset].[table_four_name]` tvr
    ON
      vr.id = tvr.vend_request_id
    JOIN
      `[dataset].[table_five_name]` au
    ON
      awt.api_id = au.id
      AND UPPER(au.type)='PREFUND'
    where awt.created_at >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS TIMESTAMP)
    order by awt.id)
    group by 1,2
    )
    --SECOND SHEET: Failed transaction yet to be refunded
    select * from api_trans where lower(type_agg) ='vend' and lower(type_agg) not in ("topup","transfer_topup", "transfer") and token_status_agg like "FAILED%"

    '''
    data = bq_execute_query(q)
    second_data = data.to_dataframe()
    print(second_data.info())
    second_data.to_csv(f"api_failed_vend_yet_to_be_reversed_{date}.csv", index = False)
    return date

# our main function
def main():
    get_data_from_bq()
    # put the files in a list
    files = [f"api_successful_vend_without_commission_{date}.csv", f"api_failed_vend_yet_to_be_reversed_{date}.csv"]
    
    # smtp library configuration
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587
    smtp_username = email
    smtp_password = password

    # structure the email
    from_email = email
    # to_email = "[receivers_email]" #if only one receiver
    to_email = ['[receivers_email_1]','[receivers_email_2]','[receivers_email_3]','[receivers_email_4]']
    subject = f'API Transaction for Auto Reversal and Commission ({date})'
    body = 'Dear All,\n\nPlease find attached the requested data.\n\nDo let me know if you need any further information.\n\nThank you.\n\nRegards '

    msg = MIMEMultipart()
    msg['From'] = from_email
    # msg['To'] = to_email # if only one email
    msg['To'] = ','.join(to_email)
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    # read files
    for file in files:
        with open(f'{file}', 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='pdf')
            attachment.add_header('Content-Disposition', 'attachment', filename=f'{file}')
            msg.attach(attachment)

    # execute to send emails
    with smtplib.SMTP(smtp_server, smtp_port) as smtp:
        smtp.starttls()
        smtp.login(smtp_username, smtp_password)
        smtp.send_message(msg)

    # delete files from folder
    for file in files:
        try:
            os.remove(file)
            print(f"File '{file}' deleted successfully.")
        except OSError as e:
            print(f"Error deleting file '{file}': {e}")

# execute the main function
if __name__ == "__main__":
    main()