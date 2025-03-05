import os
import requests
import pandas as pd
from io import StringIO
from datetime import date
from dateutil.rrule import rrule, DAILY
import psycopg2
import sqlalchemy as sa

# Connection parameters
db_config = {
    "host": "localhost", 
    "port": 5432, 
    "database": "postgres",  
    "user": "postgres", 
    "password": "postgres"  
}

connection_string = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = sa.create_engine(connection_string)


path = os.path.dirname(__file__)
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 5)

for current_date in rrule(DAILY, dtstart=start_date, until=end_date):
    dt = current_date.strftime("%Y%m%d")
    with requests.get(f'http://www.kworb.net/apple_songs/archive/{dt}.html', verify=False) as response:
        html_tables = pd.read_html(StringIO(response.text))[0]
        html_tables['date'] = current_date
        html_tables.to_csv(f'{path}/charts/apple/{dt}.csv')
        html_tables.to_sql('chart_data', engine, if_exists="replace", index=False)
        print(f"Inserted data from {dt} into {'chart_data'}")