from signal import alarm
from weatherdata_class import WeatherData
from matplotlib import pyplot as plt
from datetime import datetime
import plotly.express as px
from time import strptime
from plotly_wd_charts import Plotly_Wd_Charts

wd = WeatherData()
wd.get_data_from_csv('barca', 'data/BARCELONA_weather_2022.csv')
wd.get_data_from_csv('kyiv', 'data/kyiv_weather_2022.csv')
wd.get_data_from_csv('london', 'data/HEATHROW_weather_2022.csv')

p_charts = Plotly_Wd_Charts()
p_charts.compare_all_stations(wd)
p_charts.compare_tmin_tmax(wd)

