import os
import requests
import pandas as pd
from io import StringIO
from datetime import date
from dateutil.rrule import rrule, DAILY

path = os.path.dirname(__file__)
start_date = date(2024, 1, 1)
end_date = date(2024, 1, 5)

for current_date in rrule(DAILY, dtstart=start_date, until=end_date):
    dt = current_date.strftime("%Y%m%d")
    with requests.get(f'http://www.kworb.net/apple_songs/archive/{dt}.html', verify=False) as response:
        html_tables = pd.read_html(StringIO(response.text))[0]
        html_tables.to_csv(f'{path}/charts/apple/{dt}.csv')