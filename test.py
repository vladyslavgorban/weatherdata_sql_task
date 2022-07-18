from sqlalchemy import MetaData, DATE, NUMERIC, VARCHAR, create_engine, Table, Column, Integer, String, Text, insert
from sqlalchemy.orm import Session
from datetime import datetime

# connects to sqlite db
engine = create_engine("sqlite+pysqlite:///weather.db", echo=True, future=True)
with engine.connect() as conn:
# query = "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name;"
# with Session(engine) as session:
#     result = session.execute(query)
#     for row in result:
#         print(row[0])
#     session.commit()

    metadata_obj = MetaData()
                    
    weatherdata_station = Table(
        'test3',
        metadata_obj,
        Column('cur_date', DATE, primary_key=True),
        Column('prcp', NUMERIC(4.2)),
        Column('tmax', NUMERIC(3.1)),
        Column('tmin', NUMERIC(3.1)),
        Column('station', VARCHAR(1000), default='test3')
    ) 
    metadata_obj.create_all(engine)

# # ins = weatherdata_station.insert().values(
# #             cur_date  = datetime.strptime('2022-07-18', "%Y-%m-%d")
# #         )

    ins = insert(weatherdata_station).values(cur_date  = datetime.strptime('2022-07-18', "%Y-%m-%d"))
    result = conn.execute(ins)
    conn.commit()
# # conn.execute(ins)
# compiled = ins.compile()
# print(compiled.params)
