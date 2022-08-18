import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import os

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_url = params.csv_url
    csv_name = 'output.csv'

    os.system(f"wget {csv_url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    #Load entire csv to postgres table using chunks

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000) #yellow_tripdata_2021-01.csv

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    #load first chunk
    df.to_sql(name=table_name, con=engine, if_exists='append', index=False)

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
        

        



if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV to Postgres')

    #user, pass, host, port, db name, table name, url of csv

    parser.add_argument('--user', help='Username for Postgres')
    parser.add_argument('--password', help='Password for Postgres')
    parser.add_argument('--host', help='Host for Postgres')
    parser.add_argument('--port', help='Port for Postgres')
    parser.add_argument('--db', help='Database for Postgres')
    parser.add_argument('--table_name', help='Table name where we will write results')
    parser.add_argument('--csv_url', help='URL of csv file')

    args = parser.parse_args()

    main(args)

