import csv
import pandas as pd
from sqlalchemy import create_engine
import time
import os

def ingest_callable(user, password, host, port, db, table_name, csv_name, execution_date):

    print(table_name, " ", csv_name, " on ", execution_date)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    print('connection established successfully, inserting data...')

    #Load entire csv to postgres table using chunks
    t_start = time.time()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000) #yellow_tripdata_2021-01.csv

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #load first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

    t_end = time.time()
    print('inserted the first chunk, took %.3f second' % (t_end - t_start))

    #Load remaining chunks

    while True:
        
        t_start = time.time()
        
        try:

            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
            
            time.sleep(1)
            
            del df
            
            time.sleep(1)
            
            t_end = time.time()

            print('Inserted another chunk, took %.3f second' % (t_end - t_start))


        except StopIteration:
            print("Ingestion is done")
            break
       