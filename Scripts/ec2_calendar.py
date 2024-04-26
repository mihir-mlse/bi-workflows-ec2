import pyodbc
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
import numpy as np
from datetime import datetime, timedelta
from io import StringIO
import requests

today  = datetime.today().date()
next_day = today + timedelta(90)
print(today)


server = 'tcp:sqls-integrations-data.database.windows.net'
database = 'crm_datamart'
username = 'user_bi'
password = 'Ana1ytic$@DL!'


query =  f"""SELECT [event_id]
      ,[event_name]
      ,[inet_event_name]
      ,[event_name_long]
      ,[season_name]
      ,[season_year]
      ,[season_id]
      ,[event_type]
      ,[arena_id]
      ,[arena_name]
      ,[manifest_name]
      ,[event_date]
      ,[event_time]
      ,[event_day]
      ,[team]
      ,[add_date]
      ,[upd_datetime]
      ,[total_events]
      ,[min_events]
      ,[event_line1]
      ,[event_line2]
      ,[event_line3]
      ,[event_line4]
      ,[event_line5]
      ,[event_line6]
      ,[primary_act]
      ,[secondary_act]
      ,[major_category]
      ,[minor_category]
      ,[duration]
      ,[valid_from]
  FROM [tm_mlse_v_event] where [event_date] >= '{today}' and [event_date] <= '{next_day}'  """




cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';ENCRYPT=yes;UID='+username+';PWD='+ password)
#cursor = cnxn.cursor()


calendar = pd.read_sql(query,cnxn)


calendar = calendar[['event_id','event_time' ,'event_name','inet_event_name','team', 'season_name','event_type','season_id', 'season_year', 'arena_id', 'arena_name', 'manifest_name', 'event_date','event_line1','major_category','minor_category']].sort_values(by='event_date')


# Select events and sports which dont end in letters
filtered_calendar = calendar[
    ((calendar['major_category'] == 'SPORTS') & 
     (~calendar['event_name'].str.endswith(tuple('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz')))) |
    (calendar['major_category'] != 'SPORTS')
]

# Selecting the legit games by selecting only games starting with team names.

allowed_start_letters = ['R', 'F', 'L', 'A']
final_filtered_calendar = filtered_calendar[
    ((filtered_calendar['major_category'] == 'SPORTS') & 
     filtered_calendar['event_name'].str.startswith(tuple(allowed_start_letters))) |
    (filtered_calendar['major_category'] != 'SPORTS')
]

#added Scotiabank Arena Premium Seat 25-4-2024
arena_name_considered = ['Scotiabank Arena Live', 'Scotiabank Arena Basketball', 'Scotiabank Arena Hockey', 'BMO Field','Coca-Cola Coliseum','Scotiabank Arena Premium Seat']



final_filtered_calendar = final_filtered_calendar[final_filtered_calendar['arena_name'].isin(arena_name_considered)]
#final_filtered_calendar['event_time'] = pd.to_datetime(final_filtered_calendar['event_time'])
#final_filtered_calendar['event_time'] = final_filtered_calendar['event_time'].dt.time

final_filtered_calendar.rename(columns={"event_line1": "promoter_name"},inplace=True)

#adding to dedupe the live event!
# final_deduped = final_filtered_calendar.drop_duplicates(subset=['event_date', 'inet_event_name'])
# final_deduped = final_deduped.drop_duplicates(subset=['event_date', 'team'])

#final_filtered_calendar['promoter_name'] = 'TEST'

#print(final_filtered_calendar.columns)
#print(final_filtered_calendar.head())



csv_buffer = StringIO()
final_filtered_calendar.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)  # Go to the start of the StringIO object

# URL to upload the CSV

#url = 'https://events.mlsedigital.dev/api/events/upload'

# Staging
url = 'https://events.mlsedigital.io/api/events/upload'


# Prepare the 'file' as a tuple containing the filename, and the StringIO object
files = {'csv': ('events.csv', csv_buffer.getvalue(), 'text/csv')}

# If the API requires additional headers or authentication, add them here
headers = {
            'x-api-key': 'OxcjK3hZ6dPfAM9trdhPwHyOFzFXyCM6',
                
            }

# Make the POST request to upload the CSV file
response = requests.post(url, files=files, headers=headers)

# Check the response
if response.status_code == 200 or response.status_code == 201:
    print("CSV file uploaded successfully")
else:
    print(f"Failed to upload CSV file. Status code: {response.status_code} - Response: {response.text}")
