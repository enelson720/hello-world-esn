import sys, os, datetime
import sqlalchemy as sa
from fbprophet import Prophet
from lib.database import get_dwh_conn_engine, save_dataframe
import pandas as pd
from fbprophet import Prophet
import numpy as np
import getpass

def save_prophet_prediction(prediction_df):
  """
  Insert Prophet prediction into DWH table 'heroku.mau_forecast'.
     prediction_df: dataframe
  
  DDL of table:
  create table heroku.mau_forecast(
    date timestamp,
    yhat_upper double precision,
    yhat_lower double precision,
    yhat double precision,
    dwh_run_date date
  );
  """
  engine = get_dwh_conn_engine()
  pd.to_datetime(prediction_df['date']).apply(lambda x: x.date())
  save_dataframe(
    engine,
    prediction_df,
    'heroku.mau_forecast',
    pkeys=['date', 'dwh_run_date'],
  )


def run_prophet_prediction(train_df, prediction_size, interval_width, yearly_seasonality, dwh_run_date=None):
  """Run Prophet prediction given training dataset, return forecast dataframe
    train_df: dataframe
    prediction_size: int
    interval_width: int
    yearly_seasonality: bool
    dwh_run_date: str. The date value for `dwh_run_date`, ie: '2019-07-10'. If not provided, will default to today.
  """
  m = Prophet(
    interval_width=interval_width,
    yearly_seasonality=yearly_seasonality
  )
  m.fit(train_df)
  future = m.make_future_dataframe(periods=prediction_size)

  forecast = m.predict(future)
  forecast = forecast[-prediction_size:]
  forecast.rename(columns={'ds':'date'}, inplace=True)
  forecast = forecast[["date", "yhat_upper", "yhat_lower", "yhat"]]
  forecast["dwh_run_date"] = dwh_run_date or datetime.datetime.today().strftime('%Y-%m-%d')

  return forecast

if __name__ == '__main__':
  engine = get_dwh_conn_engine()
  with engine.connect() as conn, conn.begin():
    df = pd.read_sql("""           
    select
    event_date,
    sum(case when engagement_lifecycle_segment in (2,3,4) then num_users else null end) as MAU_agg
    from heroku.redshift_mau_details_agg
    where event_date > '2016-07-31'
    group by 1
    order by event_date asc
           """, conn)
  columns = ["date", "MAU Agg"]
  df.columns = columns
  df['date'] = pd.DatetimeIndex(df['date'])
  train_df = df.copy()
  train_df.columns = ['ds', 'y']

  forecast = run_prophet_prediction(
    train_df, prediction_size=90, interval_width=0.95, yearly_seasonality=True)
  save_prophet_prediction(forecast)
