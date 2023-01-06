from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pathlib
from os.path import exists

#API key:  OI736XXBB9GJWUC3

def make_dir(path: str, with_file=False):
    if with_file:
        path = '/'.join(path.split('/')[:-1])
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def _flat_json(json_file, csv_file, **context) -> None:
    df = pd.read_json(json_file)
    df = (df['Time Series (Daily)']
        .dropna()
        .reset_index()
        .rename(columns={"index":"date"}))
    df_money = pd.json_normalize(df['Time Series (Daily)'])

    df = df.merge(df_money, left_index=True, right_index=True)
    df = df.drop(columns=["Time Series (Daily)"])

    col = {}
    for i in df.columns:
        if i != 'date' and i != 'index':
            col[i] = i[3:]
    df = df.rename(columns=col)
    df.to_csv(csv_file, index=False)

def _append_data(csv_file, comb_file, **context):
    file_exists = exists(comb_file)
    comb = None

    if file_exists:
        comb = pd.read_csv(comb_file)
    else:
        comb = pd.DataFrame({'date': pd.Series(dtype='str'),
                            'open': pd.Series(dtype='float'),
                            'hiht': pd.Series(dtype='float'),
                            'low': pd.Series(dtype='float'),
                            'close': pd.Series(dtype='float'),
                            'adjusted close': pd.Series(dtype='float'),
                            'volume': pd.Series(dtype='int'),
                            'dividend amount': pd.Series(dtype='float'),
                            'split coefficient': pd.Series(dtype='float')})

    pd_new = pd.read_csv(csv_file)
    pd_new = pd_new[~pd_new['date'].isin(comb['date'])]
    comb = pd.concat([comb, pd_new], axis=0)
    comb.to_csv(comb_file, index=False)



my_dag = DAG("fetch_stocks",
             start_date=datetime(2023, 1, 1, 00, 00),
             schedule_interval="@daily",
             template_searchpath='/tmp_{{ ds }}_{{ next_ds }}',
             catchup=True,
             max_active_runs=1)

start_operator = DummyOperator(
    task_id='start',
    dag=my_dag
)

flat_json = PythonOperator(
    task_id="flat_json",
    python_callable=_flat_json,
    op_kwargs={
        'json_file': '/tmp/data/raw/van_full/stocks_nvidia_{{ ds }}_{{ next_ds }}.json',
        'csv_file': '/tmp/data/raw/van_flatten/stocks_nvidia_{{ ds }}_{{ next_ds }}.csv'
    }
)

dowload_desc = BashOperator(
    task_id="dowload_stock",
    bash_command='curl "https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=IBM&apikey=OI736XXBB9GJWUC3"' + 
                 ' --output /tmp/data/raw/van_full/stocks_nvidia_{{ ds }}_{{ next_ds }}.json' 
)

append_data = PythonOperator(
    task_id="append_data",
    python_callable=_append_data,
    op_kwargs={
        'comb_file': '/tmp/data/cleansed/van_comb.csv',
        'csv_file': '/tmp/data/raw/van_flatten/stocks_nvidia_{{ ds }}_{{ next_ds }}.csv'
    }
)

end = DummyOperator(task_id='end')

start_operator >> dowload_desc >> flat_json >> append_data >> end
