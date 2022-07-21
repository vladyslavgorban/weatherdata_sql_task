from matplotlib import pyplot as plt

wd_dates = [('2022-01-01',), ('2022-01-02',), ('2022-01-03',), ('2022-01-04',), ('2022-01-05',), ('2022-01-06',), ('2022-01-07',), ('2022-01-08',), ('2022-01-09',), ('2022-01-10',)]
wd_prcp = [(5.1,), (2,), (5.1,), (0,), (4.3,), (0,), (1,), (0,), (6.1,), (0,)]
wd_tmax = [(7,), (None,), (8.2,), (6.8,), (10.7,), (None,), (None,), (0.2,), (None,), (None,)]

# draw a diagramm in plt
plt.style.use('seaborn')
fig, ax = plt.subplots()
ax.plot(wd_dates, wd_prcp, c="blue")
ax.plot(wd_dates, wd_tmax, c="red")

fig.autofmt_xdate()

plt.show()