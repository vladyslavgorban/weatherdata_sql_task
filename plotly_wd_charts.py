from cProfile import label
from matplotlib import pyplot as plt
import plotly.express as px

class Plotly_Wd_Charts():
    """class for plots drawing using plotly 
    and weatherdata from WeatherData class"""

    def __init__(self) -> None:
        pass

    def compare_all_stations(self, weatherdata):
        """drow a chart with given weatherdatatype for all stations in db"""
        all_data = weatherdata.get_station_data_columns()
        while True:
            wd_to_compare = int(input('choose data to comare (prcp:1, tmax: 2, tmin: 3): '))
            if wd_to_compare in (1, 2, 3): break
        wd_to_compare = weatherdata.weatherdatatypes[wd_to_compare]

        stations = weatherdata.weather_stations_in_db()
        chart_title =''
        for sttn in stations:
            if chart_title: chart_title += f", {sttn.title()}"
            else: chart_title += f"{sttn.title()}"
        
        chart_title = f"{wd_to_compare.upper()} for " + chart_title
        labels = {
            'cur_date': 'date',
            wd_to_compare: wd_to_compare.upper()
        }
        fig = px.line(all_data, x='cur_date', y=wd_to_compare, 
                    color='station', labels=labels, title=chart_title, )
        fig.show()

    def compare_tmin_tmax(self, weatherdata):
        """drow a chart with max and min temp comparison"""
        stations = weatherdata.weather_stations_in_db()
        while True:
            i = 1
            for sttn in stations:
                print(f"#{i}: {sttn}")
                i += 1
            station = input('input station # to compare: ')
            if stations[int(station) - 1]: break
        station = stations[int(station) - 1]

        dx = weatherdata.get_station_data_columns(station)

        plt.style.use('seaborn')
        fig, ax = plt.subplots()
        ax.plot(dx['cur_date'], dx['tmax'], c="red", alpha=0.5)
        ax.plot(dx['cur_date'], dx['tmin'], c="blue", alpha=0.5)
        
        # try to fill gap between tmin & tmax if they are without breaks
        try:
            plt.fill_between(dx['cur_date'], dx['tmax'], dx['tmin'], facecolor='blue', alpha=0.1)
        except:
            pass
        # display the diagramm
        title = f"Daily high and low temperatures \n{station.title()}"
        plt.title(title, fontsize=20)
        plt.xlabel('', fontsize=16)
        fig.autofmt_xdate()
        plt.ylabel("Tenp(c)", fontsize=16)
        plt.tick_params(axis='both', which='major', labelsize=10)

        plt.show()