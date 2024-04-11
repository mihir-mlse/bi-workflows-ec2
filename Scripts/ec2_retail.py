#!/usr/bin/env python
# coding: utf-8

import pyodbc
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import numpy as np
from datetime import datetime, timedelta
import boto3
import io
from botocore.exceptions import NoCredentialsError


server = 'tcp:sqls-integrations-data.database.windows.net'
database = 'crm_datamart'
username = 'user_bi'
password = 'Ana1ytic$@DL!'


connection_string = ('DRIVER={ODBC Driver 18 for SQL Server};'
                     'SERVER=' + server + ';'
                     'DATABASE=' + database + ';'
                     'ENCRYPT=yes;'
                     'TrustServerCertificate=yes;'
                     'UID=' + username + ';'
                     'PWD=' + password + ';'
                     'PROTOCOL=TCPIP;'
                     )  # Increase as needed

cnxn = pyodbc.connect(connection_string)

cursor = cnxn.cursor()

# Day Logic
today = datetime.now()

# Calculate the next day's date by adding a timedelta of 1 day
fetch_day = today - timedelta(days=1)
fetch_day_end = today 
fetch_day_formatted = fetch_day.strftime('%Y-%m-%d')
fetch_day_end_formatted = fetch_day_end.strftime('%Y-%m-%d')
fetch_day_formatted

sql = f"select [Date] ,[Store Code],[Store Name],[Store Revenue Center],[ItemDescription],[ItemSKU],[Item Style],[Daily Item Total Sales],[Daily Item Total Retail],[Daily Item Received Cost],[Item Level],[Item Level2],[Item Level3],[Item Level4]  FROM ydg_viewDailyBIRollup_Full where DATE = '{fetch_day_formatted}'  "

item_details = pd.read_sql(sql,cnxn)
item_details = item_details[item_details['Store Code'] != 'ECOMMERCE']
item_details

server = 'mlsedb.database.windows.net,1433'
database = 'sqldb-pos-caps-006'
username = 'User_ReadOnly'
password = '28VoUVE3jxuc'


connection_string = ('DRIVER={ODBC Driver 18 for SQL Server};'
                     'SERVER=' + server + ';'
                     'DATABASE=' + database + ';'
                     'ENCRYPT=yes;'
                     'TrustServerCertificate=yes;'
                     'UID=' + username + ';'
                     'PWD=' + password + ';'
                     'PROTOCOL=TCPIP;'
                     )  # Increase as needed

cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()


sql = f"""SELECT 
    c.CheckID as check_id,
    c.CheckNumber as check_no,
    c.RevCtrID as rvc_id,
    c.CheckOpen as check_open_time,
    c.CheckClose as check_close_time,
    t.TransactionTime as transaction_time,
    c.ReopenedFromCheckNum as reopened_from_check_num,
    c.LastEmployeeID as last_employee_id,
    c.SubTotal as subtotal,
    t.Data3 as menu_item_def_id,
    t.TtlType as ttl_type,
    t.ItemCount as qty,
    t.Amount1 as amount1,
    t.Amount2 as amount2,
    t.TransID as trans_id,
    st2.StringText dining_table_id,
    mid.NluNumber as item_code,
    st1.StringText as product_name,
    e.FirstName as emp_first_name,
    e.LastName as emp_last_name,
    s8.StringText as void_reason
FROM TOTALS t
LEFT JOIN MENU_ITEM_DEFINITION mid on mid.MenuItemDefID = t.Data3
LEFT JOIN STRING_TABLE st1 on mid.Name1ID = st1.StringNumberID
LEFT Join VOID_REASON vr on vr.NameID = t.Data6 - 224
LEFT join STRING_TABLE s8 ON s8.StringNumberID = vr.NameID
LEFT JOIN CHECKS c on c.CheckID = t.CheckID
LEFT Join DINING_TABLE dt on dt.DiningTableID = c.DiningTableID
LEFT Join STRING_TABLE st2 on st2.StringNumberID = dt.NameID
INNER JOIN EMPLOYEE e on e.EmployeeID = c.LastEmployeeID

WHERE  t.TransactionTime >= '{fetch_day_formatted} 04:00:00.000' 
    AND t.TransactionTime < '{fetch_day_end_formatted} 04:00:00.000' AND t.TtlType in (11,13,15,16) """  

data = pd.read_sql(sql,cnxn)
data = data[~data['item_code'].isna()]
data['item_code'] = data['item_code'].astype('int64').astype(str)

data_timestamp = data.groupby('transaction_time')['amount1'].sum().reset_index(name='total_amount')
data_timestamp['interval_alias'] = (data_timestamp['transaction_time'].dt.floor('15T')).dt.time

item_details_refined = item_details[['ItemSKU','Item Level','Item Level2','Item Level3','Item Level4']].drop_duplicates()

final = pd.merge(data, item_details_refined, left_on='item_code', right_on = 'ItemSKU')
final

#Parquet format

from io import BytesIO
import configparser

# Load the configuration file
config = configparser.ConfigParser()
config.read('aws_credentials.conf')

aws_access_key_id = config.get('default', 'aws_access_key_id')
aws_secret_access_key = config.get('default', 'aws_secret_access_key')
region_name = config.get('default', 'region_name')


s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name,  # e.g., 'us-west-2'
    #aws_session_token = ''

)



def upload_df_to_s3(dataframe, bucket, s3_file):
    # Create a buffer
    csv_buffer = io.StringIO()
    
    # Write the DataFrame to the buffer
    dataframe.to_csv(csv_buffer, index=False)
    
    # Seek to the beginning of the StringIO object
    csv_buffer.seek(0)
    
    try:
        # Upload the buffer contents to S3
        s3_client.put_object(Bucket=bucket, Key=s3_file, Body=csv_buffer.getvalue())
        print(f"Upload successful: {s3_file}")
    except boto3.exceptions.S3UploadFailedError as e:
        print(f"Upload failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


# Replace with your S3 bucket name and desired destination file name
bucket_name = 'retail-pos-bi-stage'
s3_file_name = f'bronze/2024/retail_pos_{fetch_day_formatted}.csv'

# Upload the DataFrame directly to S3
upload_df_to_s3(final, bucket_name, s3_file_name)



# parquet_buffer = BytesIO()
# final.to_parquet(parquet_buffer, engine='pyarrow', index=False)
# parquet_buffer.seek(0)

# bucket_name = 'retail-pos-bi-stage'
# s3_file_name = f'bronze/2024/retail_pos_{fetch_day_formatted}.parquet'  # Customize your file name




# try:
#     s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=parquet_buffer.getvalue())
#     print(f"Upload successful: {s3_file_name}")
# except boto3.exceptions.S3UploadFailedError as e:
#     print(f"Upload failed: {e}")
# except Exception as e:
#     print(f"An error occurred: {e}")
