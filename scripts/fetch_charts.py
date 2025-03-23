from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import db.connection
from io import StringIO
import pandas as pd
from pathlib import Path
import requests



def fetch_kworb_charts(source: str, target: str = 'sql', start: date = None, end: date = None):
    """fetches chart data from kworb.net

    Args:
        source (str): kworb.net charts source (apple, itunes, or radio)
        target (str, optional): in which format to save the data. Defaults to 'sql'.
        start (date, optional): first date to get the charts from. Defaults to yesterday.
        end (date, optional): last date to get the charts from. Defaults to yesterday.

    Raises:
        ValueError: if the source is not valid
        ValueError: if the target is not valid
    """
    # start and end are yesterday by default to fetch the latest data
    start = datetime.today() - timedelta(days=1) if not start or start >= datetime.today() else start
    end = datetime.today() - timedelta(days=1) if not end or end >= datetime.today() else end
    
    # match sources to their actual paths on kworb.net
    match source:
        case 'apple':
            resource_path = 'apple_songs/archive'
        case 'itunes':
            resource_path = 'ww/archive'
        case 'radio':
            resource_path = 'radio/archives'
        case _:
            raise ValueError(f"Source {source} not found.")
    
    # matches our target parameter to either save as csv or in our db
    match target:
        case 'csv':
            # create the path if it doesn't exist and create save_data fn to save the csv there
            save_path = f"data/charts/{source}"
            Path(save_path).mkdir(parents=True, exist_ok=True)
            def save_data(dt: date, df: pd.DataFrame):
                df.to_csv(f"{save_path}/{dt}.csv")  
                print(f"Inserted data from {dt} into {save_path}/{dt}.csv")  
        case 'sql':
            # connect to the db and create save_data fn to save data in the corresponding table
            engine = db.connection.get_engine()
            def save_data(dt: date, df: pd.DataFrame):
                df.to_sql(f'chart_data_{source}', engine, if_exists="replace", index=False)
                print(f"Inserted data from {dt} into chart_data_{source}")
        case _:
            raise ValueError(f"Target {target} is invalid.")
                      
    # fetch and save each date between start date and end date
    for current_date in rrule(DAILY, dtstart=start, until=end):
        dt = current_date.strftime("%Y%m%d")
        with requests.get(f'http://www.kworb.net/{resource_path}/{dt}.html', verify=False) as response:
            response.encoding = 'utf-8'
            html_tables = pd.read_html(StringIO(response.text), encoding='utf-8')[0]
            html_tables['date'] = current_date
            save_data(dt, html_tables)
