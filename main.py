import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, create_engine, DateTime, Float


def load_from_csv(list_to_add, csv_path=""):
    if csv_path:
        with open(csv_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                list_to_add.append(row)
            print(f"Successfully loaded data from {csv_path}")


if __name__ == '__main__':
    engine = create_engine('sqlite:///database.db')

    meta = MetaData()

    c_measure = Table(
        'Measure', meta,
        Column('id', Integer, primary_key=True),
        Column('station', String),
        Column('date', String),
        Column('precip', Float),
        Column('tobs', Integer),
    )

    c_stations = Table(
        'Stations', meta,
        Column('id', Integer, primary_key=True),
        Column('station', String),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('elevation', Float),
        Column('name', String),
        Column('country', String),
        Column('state', String),
    )

    clean_measure = []
    clean_stations = []
    load_from_csv(clean_measure, "clean_measure.csv")
    load_from_csv(clean_stations, "clean_stations.csv")

    conn = engine.connect()
    ins = c_measure.insert()
    for row in clean_measure:
        conn.execute(ins.values(row))
    conn.close()

    conn = engine.connect()
    ins = c_stations.insert()
    for row in clean_stations:
        conn.execute(ins.values(row))
    conn.close()