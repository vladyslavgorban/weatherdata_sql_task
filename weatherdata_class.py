from distutils.util import execute
from pymysql import Date
from sqlalchemy import MetaData, DATE, NUMERIC, create_engine, Table, Column, text
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

        self.weatherdatatypes = ['cur_date', 'prcp', 'tmax', 'tmin', 'station']

    def get_data_from_csv(self, name, filepath):
        """get data from given csv filepath and store it to named table in db"""
        if name in self.metadata_obj.tables:
            print(f"Table '{name}' already exsists. Rename or delete'")
            return
        
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
                    self._get_csv_line_for_query(row)

                    # insert row for current date
                    ins = self.metadata_obj.tables[name].insert().values(
                        cur_date = self._currentdate,
                        prcp = self._prcp,
                        tmax = self._tmax,
                        tmin = self._tmin
                    )
                    conn.execute(ins)
            
                # commit inserts
                conn.commit()
    
    def _create_table(self, name):
        """cteate table for weatherdata with given name"""
        # create table
        if name in self.metadata_obj.tables:
            print(f"Table '{name}' already exsists. Skipped. Rename or delete")
        else:
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

    def _get_csv_line_for_query(self, row):
        """read the csv line and prepare variables for `query`"""
        # get data as primaty key
        self._currentdate = datetime.strptime(row[self.data_columns['date']], "%Y-%m-%d")

        # get pscp if exists, else = 0
        try:
            self._prcp = float(row[self.data_columns['prcp']])
        except ValueError:
            self._prcp = 0
        
        # get tmax if exist
        try:
            self._tmax = float(row[self.data_columns['tmax']])
        except ValueError:
            self._tmax = None

        # get tmin if exist
        try:
            self._tmin = float(row[self.data_columns['tmin']])
        except ValueError:
            self._tmin = None

    def _join_weatherdata_query(self):
        """join all weather data in one table row by row"""
        table_name = ""
        query = "SELECT * FROM ("
        for table in self.metadata_obj.tables:
            if table_name: query += " UNION ALL "
            table_name = str(table)
            query += f"SELECT *, '{table_name}' FROM {table_name}"
        query += ") ORDER BY cur_date;"

        return query

    def join_weatherdata_rows(self, header_line=False):
        """join all weather data in one table row by row lists"""
        query = self._join_weatherdata_query()
        
        if header_line: 
            weather_data_rows = [['cur_date', 'prcp', 'tmax', 'tmin', 'station']]
        else: 
            weather_data_rows = []
                
        # connect to db and go row by row
        with self.engine.connect() as conn:
       
            for row in conn.execute(text(query)):
                weather_data_row = []
                for wd_data in range(len(row)):
                    # set correct type:
                    if self.weatherdatatypes[wd_data] == 'cur_date':
                        value = datetime.strptime(row[wd_data], "%Y-%m-%d")
                    else:
                        try:
                            value = float(row[wd_data])
                        except:
                            value = None
                    weather_data_row.append(value)
                weather_data_rows.append(weather_data_row)

        return weather_data_rows

    def get_station_data_columns(self, station_name=None):
        """dict with weather data for all db or given station"""

        weather_data_columns = {name: list() for name in self.weatherdatatypes}

        if station_name:
            query = f"SELECT * FROM {station_name};"
        else:
            query = self._join_weatherdata_query()
        
        with self.engine.connect() as conn:
            for row in conn.execute(text(query)):
                for wd_data in range(len(row)):
                    # set correct type:
                    if self.weatherdatatypes[wd_data] == 'cur_date':
                        value = datetime.strptime(row[wd_data], "%Y-%m-%d")
                    elif self.weatherdatatypes[wd_data] == 'station':
                        value = row[wd_data]
                    else:
                        try:
                            value = float(row[wd_data])
                        except:
                            value = None

                    weather_data_columns[self.weatherdatatypes[wd_data]].append(value)

        return weather_data_columns

    def weather_stations_in_db(self):
        """return list of tables in db"""
        stations = list()
        for table in self.metadata_obj.tables:
            table_name = str(table)
            stations.append(table_name)
        return stations   