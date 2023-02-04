
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    output_file = "output.csv"
    
    if not os.path.exists("output.csv"):
        os.system(f"curl {url} -o {output_file}")

    trip_data = pd.read_parquet(f"{output_file}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    engine.connect()

    chunk_trip_data = trip_data[:200000] # first 1 million data

    chunk_trip_data.to_sql(name=f'{table_name}',if_exists='replace'
                        ,con = engine)

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Ingest data to postgres')
    # user
    parser.add_argument('--user',help='username for the postgres')
    # password
    parser.add_argument('--password',help='password for the postgres')
    # host
    parser.add_argument('--host', help='host address for the postgres')
    # port
    parser.add_argument('--port',help='port address for the postgres')
    # database name
    parser.add_argument('--db',help='database name for postgres')
    # table name
    parser.add_argument('--table_name',help='name of the table where we will output the results')
    # url of the csv
    parser.add_argument('--url',help='url of the csv file')

    args = parser.parse_args()
    main(args)