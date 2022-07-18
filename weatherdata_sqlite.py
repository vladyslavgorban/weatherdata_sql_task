"""fill in the station table in db"""

from distutils.util import execute
from pymysql import Date
from sqlalchemy import MetaData, DATE, NUMERIC, VARCHAR, create_engine, Table, Column, Integer, String, Text, insert
import csv
from datetime import datetime

def add_weather_station(filepath, station_name=None):
    """
    open CSV and store data to dict named by station
    sent data to tables in SQLite db
    """

    # connects to sqlite db
    engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)
    conn = engine.connect()

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

                # create table
                metadata_obj = MetaData()
                
                weatherdata_station = Table(
                    city_name,
                    metadata_obj,
                    Column('cur_date', DATE, primary_key=True),
                    Column('prcp', NUMERIC(4.2)),
                    Column('tmax', NUMERIC(3.1)),
                    Column('tmin', NUMERIC(3.1)),
                    Column('station', VARCHAR(1000), default=city_name)
                ) 
                metadata_obj.create_all(engine)
            
            # get data as primaty key
            currentdate = datetime.strptime(row[data_columns['date']], "%Y-%m-%d")

            # get pscp if exists, else = 0
            try:
                prcp = float(row[data_columns['prcp']])
            except ValueError:
                prcp = 0
                # print(f"no prcp for {currentdate}")
            
            # get tmax if exist
            try:
                tmax = float(row[data_columns['tmax']])
            except ValueError:
                tmax = None
                tmax_insert_query = ''
                # print(f"no tmax for {currentdate}")
            else:
                # tmax = f", {tmax}"
                tmax_insert_query = f", tmax={tmax}"

             # get tmin if exist
            try:
                tmin = float(row[data_columns['tmin']])
            except ValueError:
                tmin = None
                tmin_insert_query = ''
                # print(f"no tmin for {currentdate}")
            else:
                # tmin = f", {tmin}"
                tmin_insert_query = f", tmin={tmin}"

            # prepare query to add data to table
            # query_1 = "cur_date = currentdate, prcp = prcp"
            # query_2 = f"{tmax_insert_query}{tmin_insert_query}"
            # query = query_1 + query_2
            # print(query)

            ins = weatherdata_station.insert().values(
                cur_date = currentdate,
                prcp = prcp,
                tmax = tmax,
                tmin = tmin
            )
            # ins = insert(weatherdata_station).values(query)

            r = conn.execute(ins)
            print(r.inserted_primary_key)

if __name__ == '__main__':
    add_weather_station('data/kyiv_weather_2022.csv', station_name='test')