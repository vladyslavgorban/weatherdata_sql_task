from distutils.util import execute
from select import select
from pymysql import Date
from sqlalchemy import MetaData, DATE, NUMERIC, VARCHAR, create_engine, Table, Column, Integer, String, Text, insert, func, cast, text
import csv
from datetime import datetime

class WeatherData():
    """class wich contains all weather data db's from CSV 
    on site https://www.ncdc.noaa.gov/ """

    def __init__(self):
        """initialize metadata object for db's"""
        
        self.engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)
        # set metadata object for all data
        self.metadata_obj = MetaData(bind=self.engine)
        # include all existind db's
        self.metadata_obj.reflect()

    def get_data_from_csv(self, name, filepath):
        """get data from given csv filepath and store it to named table in db"""
        # create table
        self._create_table(name)

        # read data from CSV
        with open(filepath) as f:
            self.reader = csv.reader(f)
            self.header_row = next(self.reader)

            # define column numbers for datatype in csv
            self._define_csv_col_number()
        
            # prepare to insert rows in db
            with self.engine.connect() as conn:
                # going through CVS line by line, send data to db table
                for row in self.reader:
                    # get data as primaty key
                    currentdate = datetime.strptime(row[self.data_columns['date']], "%Y-%m-%d")

                    # get pscp if exists, else = 0
                    try:
                        prcp = float(row[self.data_columns['prcp']])
                    except ValueError:
                        prcp = 0
                    
                    # get tmax if exist
                    try:
                        tmax = float(row[self.data_columns['tmax']])
                    except ValueError:
                        tmax = None

                    # get tmin if exist
                    try:
                        tmin = float(row[self.data_columns['tmin']])
                    except ValueError:
                        tmin = None

                    # insert row for current date
                    ins = self.metadata_obj.tables[name].insert().values(
                        cur_date = currentdate,
                        prcp = prcp,
                        tmax = tmax,
                        tmin = tmin
                    )
                    conn.execute(ins)
            
                # commit inserts
                conn.commit()
    
    def _create_table(self, name):
        """cteate table for weatherdata with gien name"""
        # create table
        Table(
            name,
            self.metadata_obj,
            Column('cur_date', DATE, primary_key=True),
            Column('prcp', NUMERIC(4.2)),
            Column('tmax', NUMERIC(3.1)),
            Column('tmin', NUMERIC(3.1))
        ) 
        self.metadata_obj.create_all(self.engine)

    def _define_csv_col_number(self):
        """define which column in csv correcponds to weather datatype"""
        # prepare dic to store column nubers
        self.data_columns = {
            'name': '',
            'date': '',
            'prcp': '',
            'tmax': '',
            'tmin': ''
        }

        for col_num in range(len(self.header_row)):
            if self.header_row[col_num] == 'NAME':
                self.data_columns['name'] = col_num
            elif self.header_row[col_num] == 'DATE':
                self.data_columns['date'] = col_num
            elif self.header_row[col_num] == 'PRCP':
                self.data_columns['prcp'] = col_num
            elif self.header_row[col_num] == 'TMAX':
                self.data_columns['tmax'] = col_num
            elif self.header_row[col_num] == 'TMIN':
                self.data_columns['tmin'] = col_num

    def _get_csv_line_for_query(self):
        """read the csv line and prepare da"""

    def join_weatherdata(self):
        """join all weather data in one table???"""

if __name__ == '__main__':
    wd = WeatherData()
    # wd.get_data_from_csv('barca', 'data/BARCELONA_weather_2022.csv')
    # wd.get_data_from_csv('kyiv', 'data/kyiv_weather_2022.csv')
    # wd.get_data_from_csv('london', 'data/HEATHROW_weather_2022.csv')
    
    with wd.engine.connect() as conn:
        i = 0
        for row in conn.execute(text("select * from kyiv")):
            print(row)
            i += 1
            if i > 10: break
