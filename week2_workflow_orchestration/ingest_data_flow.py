
import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time
import os
from prefect import flow,task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True, retries=3,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def extract_data(url):
    output_file = "output.csv"
    
    if not os.path.exists("output.csv"):
        os.system(f"curl {url} -o {output_file}")

    trip_data = pd.read_parquet(f"{output_file}")
    
    chunk_trip_data = trip_data[:200000] # first 2 million data
    return chunk_trip_data

@task(log_prints=True, retries=3)
def tranform_data(data):
    assert isinstance(data,pd.DataFrame)
    print(f"Pre: Missing passenge count : {data['passenger_count'].isin([0]).sum()}")
    data = data[data['passenger_count']!=0] 
    print(f"Post: Missing passenge count : {data['passenger_count'].isin([0]).sum()}")

    return data

@task(log_prints=True, retries=3)
def ingest_data(table_name,data):

    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        data.to_sql(name=f'{table_name}',if_exists='replace',con = engine)

# @flow(name="sub Flow")
# def sub_flow(table_name):
#     print(f"{table_name} has been created")

@flow(name="ingest Flow")
def main_flow(table_name):
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-11.parquet"

    raw_data = extract_data(url)
    clean_data = tranform_data(raw_data)
    ingest_data (table_name,clean_data)

if __name__=='__main__':
    main_flow("yellow_taxi_data_22")
    
