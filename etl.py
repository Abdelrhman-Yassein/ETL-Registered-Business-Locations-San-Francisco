import pandas as pd
import psycopg2 as ps
from sql_queries import *
import datetime


def read_data():
    df = pd.read_csv(
        "data/Registered_Business_Locations_-_San_Francisco.csv", low_memory=False
    )
    return df


def transform_data(df):

    # 1: Update Column Name
    # convert columns names to lower case and update ' ' to be unnderScoar
    columns_names = (['_'.join(x.lower().split(' ')) for x in df.columns])
    # update columns names
    df.set_axis(columns_names, axis=1, inplace=True)

    # 2: Convert location_id to BE Index
    df.drop_duplicates(subset='location_id', keep='first', inplace=True)
    df.set_index('location_id', inplace=True)

    # 3: Convert Date Column to Date Object
    dates_columns = ['business_start_date', 'business_end_date',
                     'location_start_date', 'location_end_date']
    for x in dates_columns:
        df[x] = pd.to_datetime(df[x])

    # 4: Clean and Update City Value
    df.dropna(subset=['city'], how='any', inplace=True)
    dfcity = df[(df['city'].str.contains("Francisco"))]
    df['city'].mask(dfcity['city'] != 'San Francisco',
                    'San Francisco', inplace=True)
    df.drop(df[df['city'].str.contains('[0-9]') == True].index, inplace=True)
    # 5: Update Columns Name
    # DBA Name =>	Doing Business as Name or Location Name	text
    # update dba_name to location_name
    df.rename(columns={'dba_name': 'location_name'}, inplace=True)
    return df


def clean_null_data(df):
    # delete Column Contain more than 50000 null value
    for col in df.columns:
        if df[col].isnull().sum() > 50000:
            df.drop(col, axis=1, inplace=True)
    # delete Row Contain more than 0 null value
    for col in df.columns:
        if df[col].isnull().sum() > 0:
            df.dropna(subset=[col], how='any', inplace=True)
    return df            


def insert_location_date_data_table(df, cur, conn):
    # 1: locationtime Table
    # select column data
    loc_time = df['location_start_date']
    # extract hour, day, week, month, year,day_name
    location_time_date = []
    for date in loc_time:
        location_time_date.append([date, date.hour, date.day, date.week, date.month, date.year, date.day_name()])
    column_labels = ('location_start_date', 'hour', 'day','week', 'month', 'year', 'weekday')
    # convert to dataframe
    loc_time_df = pd.DataFrame.from_records(location_time_date, columns=column_labels)
    # select just 10000 rows
    loc_time_df = loc_time_df[:10000]
    # insert into database
    for i, row in loc_time_df.iterrows():
        cur.execute(insert_locationtime_table, list(row))
        conn.commit()


def insert_businesstime_table(df, cur, conn):
    # select column data
    business_time = df['business_start_date']
    # extract hour, day, week, month, year,day_name
    business_start_date = []
    for date in business_time:
        business_start_date.append(
            [date, date.hour, date.day, date.week, date.month, date.year, date.day_name()])
    column_labels = ('location_start_date', 'hour', 'day',
                     'week', 'month', 'year', 'weekday')
    # convert to dataframe
    business_time_df = pd.DataFrame.from_records(
        business_start_date, columns=column_labels)
    # select just 10000 rows
    business_time_df = business_time_df[:10000]
    # insert into database
    for i, row in business_time_df.iterrows():
        cur.execute(insert_businesstime_table, list(row))
        conn.commit()


def insert_registeredbusiness_table(df, cur, conn):
    # select Columns
    registeredbusiness = df[['uniqueid', 'business_account_number', 'ownership_name',
                             'parking_tax', 'transient_occupancy_tax',
                             'business_location', 'location_name', 'business_start_date', 'location_start_date']]
    # select just 10000 rows
    registeredbusiness = registeredbusiness[:10000]
    # insert into database
    for i, row in registeredbusiness.iterrows():
        cur.execute(insert_registeredbusiness_table, list(row))
        conn.commit()


def insert_location_table(df, cur, conn):
    # select Columns
    location_df = df[['location_name', 'street_address',
                      'city', 'state', 'source_zipcode']]
    # select just 10000 rows
    location_df = location_df[:10000]
    # insert into database
    for i, row in location_df.iterrows():
        cur.execute(insert_location_table, list(row))
        conn.commit()


def main():
    """
    Function used to extract, transform all data from Registered_Business_Locations_-_San_Francisco 
    and load it into a PostgreSQL DB
    Usage: python etl.py
    """
    # Create DataBase Connection
    print("Connect To DataBase ......")
    conn = ps.connect(
        "host=127.0.0.1 dbname=businesslocation  user=postgres password=postgre")
    cur = conn.cursor()
    print("Connect successfully  ")

    print("Read Data ......")
    df = read_data()
    print("Transformation Data ......")
    df = transform_data(df)
    print("Clean Null Data ......")
    df = clean_null_data(df)
    print("Insert location_start_date Data ......")
    insert_location_date_data_table(df, cur, conn)
    print("Insert businesstime Data ......")
    insert_businesstime_table(df, cur, conn)
    print("Insert location Data ......")
    insert_location_table(df, cur, conn)
    print("Insert registeredbusiness Data ......")
    insert_registeredbusiness_table(df, cur, conn)
    print('Connection Close')
    conn.close()
    print(" Congratlaution Finshed ETL Proccess :)")

if __name__ == '__main__':
    main()
