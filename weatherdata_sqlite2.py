"""fill in the station table in db"""

from sqlalchemy import MetaData, DATE, NUMERIC, VARCHAR, create_engine, Table, Column, Integer, String, Text, insert
from sqlalchemy.orm import Session
import csv
from datetime import datetime

def add_weather_station(filepath, station_name):
    """
    open CSV and store data to dict named by station
    sent data to tables in SQLite db
    """

    # connects to sqlite db
    engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)
    with Session(engine) as session:

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
                    city_name = station_name
                    # if station_name:
                    #     # if name was defined
                    #     city_name = station_name
                    # else:
                    #     # else take first word from station name in CSV
                    #     city_name = row[data_columns['name']].split()[0]                 

                    # create table
                    query_create_table = f"CREATE TABLE {city_name} (cur_date date primary key, prcp numeric(4.2), tmax numeric(3.1), tmin numeric(3.1),station varchar(100) default '{city_name}');"
                    session.execute(query_create_table)

                # get data as primaty key
                currentdate = datetime.strptime(row[data_columns['date']], "%Y-%m-%d")

                # get pscp if exists, else = 0
                try:
                    prcp = float(row[data_columns['prcp']])
                except ValueError:
                    prcp = 0
                
                # get tmax if exist, prepare to query text
                try:
                    tmax = float(row[data_columns['tmax']])
                except ValueError:
                    tmax = ''
                    tmax_insert_query = ''
                else:
                    tmax = f", {tmax}"
                    tmax_insert_query = ", tmax"

                # get tmin if exist, prepare to query text
                try:
                    tmin = float(row[data_columns['tmin']])
                except ValueError:
                    tmin = ''
                    tmin_insert_query = ''
                else:
                    tmin = f", {tmin}"
                    tmin_insert_query = ", tmin"

                # prepare query to add data to table
                query_insert_names = "cur_date, prcp" + tmax_insert_query + tmin_insert_query
                query_insert_values = f"'{currentdate}', {prcp}{tmax}{tmin}"
                query_insert = f"INSERT INTO {city_name}({query_insert_names}) VALUES({query_insert_values});"
                # print(query_insert)

                session.execute(query_insert)

        session.commit()        

if __name__ == '__main__':
    add_weather_station('data/kyiv_weather_2022.csv', 'kyiv')