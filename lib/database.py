#!/usr/bin/env python

import sys, os, datetime
import sqlalchemy as sa
import time
import pandas as pd
from lib.helpers import log


def get_dwh_conn_engine(echo=False):
  "Obtain SQLAlchemy engine connection to the Redshift DWH"
  conn_str = os.getenv('HEROKU_PREDICTION_REDSHIFT_URL')
  if not conn_str:
    raise Exception('Env Var HEROKU_PREDICTION_REDSHIFT_URL not found. Please run in correct environment.')
  return sa.create_engine(conn_str, echo=echo)

def dwh_conn_execute(query, as_df=False):
  "Connect to Redshift DWH, execute query, return result and close connection. as_df means return as dataframe"
  engine = get_dwh_conn_engine()
  with engine.connect() as conn, conn.begin():
    return pd.read_sql(query, conn) if as_df else conn.execute(query)

def get_pg_conn_engine(echo=False):
  "Obtain SQLAlchemy engine connection to the PG"
  conn_str = os.getenv('PRIMARY_DATABASE_URL')
  if not conn_str:
    raise Exception('Env Var PRIMARY_DATABASE_URL not found. Please run in correct environment.')
  return sa.create_engine(conn_str, echo=echo)

def pg_conn_execute(query, as_df=False):
  "Connect to PG, execute query, return result and close connection. as_df means return as dataframe"
  engine = get_pg_conn_engine()
  with engine.connect() as conn, conn.begin():
    return pd.read_sql(query, conn) if as_df else conn.execute(query)

def save_dataframe(engine, df, table_name, pkeys=[], temp_schema='staging'):
  """Insert/Update dataframe into a DWH table based on primary keys (if provided). Table must exist.
     engine: sqlachemy engine of destination database
     df: dataframe of data to be inserted
     table_name: string of the table and schema. Example: staging.mau_forecast
     pkeys: list of primary key columns
     temp_schema: string of the schema to put a temp table in. Default is 'staging'.
  """
  records = df.to_dict('records')
  schema, table = table_name.split('.')
  meta = sa.MetaData(bind=engine)
  

  with engine.connect() as conn, conn.begin() as trans:
    if len(pkeys) > 0:
      # create temp
      engine.execute(f'DROP TABLE IF EXISTS {temp_schema}.{table}')
      engine.execute(f'CREATE TABLE {temp_schema}.{table} AS SELECT * from {table_name} where 1=0')

      dest_table_temp = sa.Table(
        table, meta, autoload=True,
        autoload_with=engine, schema=temp_schema,
      )

      # insert to 
      engine.execute(dest_table_temp.insert(), records)

      # delete existing
      where_str = ' AND '.join([f'{schema}.{table}.{col} = {temp_schema}.{table}.{col}' for col in pkeys])
      
      engine.execute(f'''DELETE FROM {schema}.{table}
      USING {temp_schema}.{table}
      WHERE {where_str}''')

      engine.execute(f'''INSERT INTO {schema}.{table}
      SELECT * FROM {temp_schema}.{table}''')

      engine.execute(f'DROP TABLE IF EXISTS {temp_schema}.{table}')
    else:
      dest_table = sa.Table(
        table, meta, autoload=True,
        autoload_with=engine, schema=schema,
      )
      engine.execute(dest_table.insert(), records)
    
    trans.commit()
    log(f'inserted {len(records)} records')
