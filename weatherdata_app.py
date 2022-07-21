from signal import alarm
from weatherdata_class import WeatherData
from matplotlib import pyplot as plt
from datetime import datetime
import plotly.express as px
from time import strptime

wd = WeatherData()
# wd.get_data_from_csv('barca', 'data/BARCELONA_weather_2022.csv')
# wd.get_data_from_csv('kyiv', 'data/kyiv_weather_2022.csv')
# wd.get_data_from_csv('london', 'data/HEATHROW_weather_2022.csv')

# all_data = wd.join_weatherdata_columns_dict()
# while True:
#     wd_to_compare = int(input('choose data to comare (prcp:1, tmax: 2, tmin: 3): '))
#     if wd_to_compare in (1, 2, 3): break
# wd_to_compare = wd.weatherdatatypes[wd_to_compare]
# fig = px.line(all_data, x='cur_date', y=wd_to_compare, color='station')
# fig.show()

wd_barca = wd.get_station_data_columns('barca')
print("table cell is: ", wd_barca['tmax'][2])
print("table cell type is: ", type(wd_barca['tmax'][2]))

# plt.style.use('seaborn')
# fig, ax = plt.subplots()
# ax.plot(wd_barca['cur_date'], wd_barca['tmax'], c="red", alpha=0.5)
# ax.plot(wd_barca['cur_date'], wd_barca['tmin'], c="blue", alpha=0.5)
# # plt.fill_between(wd_barca['cur_date'], wd_barca['tmax'], wd_barca['tmin'], facecolor='blue', alpha=0.1)
# # display the diagramm
# title = "Daily high and low temperatures \nBarcelona, Spain"
# plt.title(title, fontsize=20)
# plt.xlabel('', fontsize=16)
# fig.autofmt_xdate()
# plt.ylabel("Tenp(f)", fontsize=16)
# plt.tick_params(axis='both', which='major', labelsize=10)

# plt.show()
