"""fill in the station table in db"""

from sqlalchemy import create_engine
import csv
from datetime import datetime

def add_weather_station(self, filepath, station_name=None):
    """
    open CSV and store data to dict named by station
    sent data to tables in SQLite db
    """

    engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)

    with open('data/kyiv_weather_2022.csv') as f:
        reader = csv.reader(f)
        header_row = next(reader)

        # define column numbers for datatype in csv
        data_columns = {
            'name': '',
            'date': '',
            'prcp': '',
            'tmax': '',
            'tmin': ''
        }
        for col_num in range(len(header_row)):
            if header_row[col_num] == 'NAME':
                data_columns['name'] = col_num
            elif header_row[col_num] == 'DATE':
                data_columns['date'] = col_num
            elif header_row[col_num] == 'PRCP':
                data_columns['prcp'] = col_num
            elif header_row[col_num] == 'TMAX':
                data_columns['tmax'] = col_num
            elif header_row[col_num] == 'TMIN':
                data_columns['tmin'] = col_num

        # going through CVS line by line, send data to db table
        city_name = '' # variable for station name
        for row in reader:
            
            # define city_name for table name and city data (only once)
            if not city_name:
                if station_name:
                    # if name was defined
                    city_name = station_name
                else:
                    # else take first word from station name in CSV
                    city_name = row[data_columns['name']].split()[0] 
            
            # get data as primaty key
            currentdate = datetime.strptime(row[data_columns['date']], "%Y-%m-%d")

            # get pscp if exists, else = 0
            try:
                prcp = float(row[data_columns['prcp']])
            except ValueError:
                prcp = 0
            
            # get tmax if exist
            try:
                tmax = float(row[data_columns['tmax']])
            except ValueError:
                tmax = ''
                tmax_insert_query = ''
            else:
                tmax = f", {tmax}"
                tmax_insert_query = f", {tmax}"

             # get tmin if exist
            try:
                tmin = float(row[data_columns['tmin']])
            except ValueError:
                tmin = ''
                tmin_insert_query = ''
            else:
                tmin = f", {tmin}"
                tmin_insert_query = f", {tmin}"

            # prepare query to add data to table
            query_insert = f"cur_date, prcp{tmax_insert_query}{tmin_insert_query}"
            query_values = f"{currentdate}, {prcp}{tmax}{tmin}"
            add_line_query = f"INSERT INTO {city_name}({query_insert}) VALUES({query_values})"