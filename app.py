"""get data from CSV files in Data folder (downoaded from https://www.ncdc.noaa.gov/), 
save data to SQLIte db, extract for further charts, draw different type of charts"""

from weatherdata_class import WeatherData
from plotly_wd_charts import Plotly_Wd_Charts

# weatherdata instance, add data from csv
wd = WeatherData()
wd.get_data_from_csv('barca', 'data/BARCELONA_weather_2022.csv')
wd.get_data_from_csv('kyiv', 'data/kyiv_weather_2022.csv')
wd.get_data_from_csv('london', 'data/HEATHROW_weather_2022.csv')

# draw two charts nypes
p_charts = Wd_Charts()
p_charts.compare_all_stations(wd)
p_charts.compare_tmin_tmax(wd)
