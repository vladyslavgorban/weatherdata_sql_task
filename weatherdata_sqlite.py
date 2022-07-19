"""fill in the station table in db"""

from distutils.util import execute
from pymysql import Date
from sqlalchemy import MetaData, DATE, NUMERIC, VARCHAR, create_engine, Table, Column, Integer, String, Text, insert
import csv
from datetime import datetime

def add_weather_station(filepath, station_name):
    """
    open CSV and store data to dict named by station
    sent data to tables in SQLite db
    """
        
    # connects to sqlite db
    engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)
    
    # create table
    metadata_obj = MetaData()
    
    weatherdata_station = Table(
        station_name,
        metadata_obj,
        Column('cur_date', DATE, primary_key=True),
        Column('prcp', NUMERIC(4.2)),
        Column('tmax', NUMERIC(3.1)),
        Column('tmin', NUMERIC(3.1))
    ) 
    metadata_obj.create_all(engine)

    # read data from CSV
    with open(filepath) as f:
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

        
        # prepare to insert rows in db
        with engine.connect() as conn:
            # going through CVS line by line, send data to db table
            for row in reader:
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
                    tmax = None

                # get tmin if exist
                try:
                    tmin = float(row[data_columns['tmin']])
                except ValueError:
                    tmin = None

                # insert row for current date
                ins = weatherdata_station.insert().values(
                    cur_date = currentdate,
                    prcp = prcp,
                    tmax = tmax,
                    tmin = tmin
                )
                r = conn.execute(ins)
           
            # commit inserts
            conn.commit()

if __name__ == '__main__':
    add_weather_station('data/kyiv_weather_2022.csv', station_name='kyiv')
    add_weather_station('data/BARCELONA_weather_2022.csv', station_name='barca')
    add_weather_station('data/HEATHROW_weather_2022.csv', station_name='london')