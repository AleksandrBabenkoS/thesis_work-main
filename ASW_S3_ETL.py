''' Построить airflow пайплайн выгрузки ежедневных отчётов по количеству поездок на велосипедах в городе Нью-Йорк.
Дипломная работа Бабенко Александра.
'''

from datetime import datetime, timedelta
import os
import boto3
import pandas as pd
import pandahouse as ph
import zipfile
import csv
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from botocore.exceptions import ClientError
import pandahouse as ph
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
client = Client('localhost')

DB_NAME = 'tripDB'
HOST = 'http://localhost:8123'
USER = 'default'
PASSWORD = ''

FROM_BUCKET = 'netobucket'
TO_BUCKET = 'netobucketreports'

    """
    Чтение учетных данных AWS S3 из файла .csv
    """
def read_keys():
    
    with open('airflow_accessKeys.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='|')
        next(reader, None)
        for row in reader:
            access_key = row[0]
            secret_key = row[1]
        logging.info('AWS S3 credentials has been received')
        return access_key, secret_key

    """
    Получить текущий список файлов в S3
    """
def get_files_list_in_bucket(s3):
    
    current_files = []
    logging.info('The list of files in the bucket today: ')
    for key in s3.list_objects(Bucket=FROM_BUCKET)['Contents']:
        current_files.append(key['Key'])
        logging.info(key['Key'])
    return current_files

    """
    Сравниваем новый список в корзине со старым, находим новые записи
    """
def search_for_new_files(current_files, yesterday_in_bucket):
    
    new_files = []
    logging.info('The list of the new files: ')
    for file in current_files:
        if file not in yesterday_in_bucket:
            new_files.append(file)
            logging.info(file)
    return new_files

    """
    Загрузка .zip-архива новых файлов в корзину и разархивирование .csv.
    """
def download_new_files_as_csv(s3, new_files):

    new_files_csv = []
    for new_file in new_files:
        s3.download_file(Bucket='netobucket',
                         Key=new_file,
                         Filename='archive.zip')
        with zipfile.ZipFile('archive.zip', 'r') as archive:
            archive.extractall('./datafiles')
            list_of_files = archive.namelist()
            if len(list_of_files) > 1:
                logging.warning('There are more than one file in the archive!')
            if len(list_of_files) < 1:
                logging.warning('There are no any files in the archive!')
            csv_file_name = list_of_files[0]
            log_message = 'New file has been downloaded and unzipped: ' + csv_file_name
            logging.info(log_message)
            new_files_csv.append(csv_file_name)
            archive.close()
        os.remove('archive.zip')
    return new_files_csv

    """
    Новая таблица с ежедневными отчетами за текущий месяц.
    """
def create_clickhouse_table(file_name):
    
    table_name = 'trips' + file_name[:6]
    request = 'CREATE TABLE tripDB.' + table_name +' (\
    date Date, tripduration Int32, gender Int16)\
    ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 8192 ;'
    client.execute(request)
    log_message = 'table created with name: ' + table_name
    logging.info(log_message)
    return table_name

    """
    Загрузка в Clickhouse через pandas Data Frame.
    """
def insert_data_to_clickhouse(csv_file, table):
    
    file_link = './datafiles/' + csv_file
    df = pd.read_csv(file_link)
    logging.info('initial data frame created')

    df['date'] = pd.to_datetime(df['starttime']).dt.floor('d')
    df_for_cklickhouse = df[['date', 'tripduration', 'gender']].copy()
    logging.info('data frame created')

    connection = dict(database=DB_NAME,
                      host=HOST,
                      user=USER,
                      password=PASSWORD)
    ph.to_clickhouse(df_for_cklickhouse, table, index=False, chunksize=100000, connection=connection)
    message = 'Data Frame loaded to Clickhouse table: ' + table
    logging.info(message)

    """
    Получение отчетов от Clickhouse через SQL-запрос.
    """
def SQL_requests(table):
   
    trips_count_list = client.execute('SELECT COUNT(date), date \
    FROM tripDB.' + table + ' \
    GROUP BY date;')
    trips_count_df = pd.DataFrame.from_records(trips_count_list, columns=['trips', 'date'])
    logging.info('number of trips found')

    trips_duration_list = client.execute('SELECT ROUND(AVG(tripduration), 3), date \
    FROM tripDB.' + table + ' \
    GROUP BY date;')
    trips_duration_df = pd.DataFrame.from_records(trips_duration_list, columns=['duration', 'data'])
    logging.info('duration found')

    gender_0_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=0 \
    GROUP BY date ;')
    gender_1_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=1 \
    GROUP BY date ;')
    gender_2_list = client.execute('SELECT COUNT(gender), date \
    FROM tripDB.' + table + ' \
    WHERE gender=2 \
    GROUP BY date ;')
    gender_0_df = pd.DataFrame.from_records(gender_0_list, columns=['gender_0', 'data'])
    gender_1_df = pd.DataFrame.from_records(gender_1_list, columns=['gender_1', 'data'])
    gender_2_df = pd.DataFrame.from_records(gender_2_list, columns=['gender_2', 'data'])
    logging.info('gender distribution found')

    report_df = trips_count_df[['date', 'trips']].copy()
    report_df['duration'] = trips_duration_df['duration'].copy()
    report_df['gender_0_%'] = round(gender_0_df['gender_0'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    report_df['gender_1_%'] = round(gender_1_df['gender_1'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    report_df['gender_2_%'] = round(gender_2_df['gender_2'] / (gender_0_df['gender_0'] +
                                                               gender_1_df['gender_1'] +
                                                               gender_2_df['gender_2']) * 100, 2)
    logging.info('Data Frame with report is ready')

    return report_df


def upload_report_to_S3_bucket(s3, report_df, table):
    
    file_name = './datafiles/report_' + table + '.csv'
    report_df.to_csv(file_name)
    message = 'Report saved locally as ' + file_name
    logging.info(message)

    key = 'report_' + table + '.csv'
    try:
        response = s3.upload_file(file_name, 'netobucketreports', key)
        logging.info('File uploaded to the AWS S3 bucket')
    except ClientError as e:
        logging.warning(e)
    except FileNotFoundError as e:
        logging.warning(e)


def drop_clickhouse_table(table):
    
    request = 'DROP TABLE tripDB.' + table
    client.execute(request)
 
   """
   Функция, которая вызывает все необходимое для выполнения процесса ETL
   """
def ETL():
   
    logging.info('Starting the task')

        access_key, secret_key = read_keys()

    yesterday_in_bucket = Variable.get("files_in_bucket")

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    current_files = get_files_list_in_bucket(s3)
    new_files = search_for_new_files(current_files, yesterday_in_bucket)
    new_files_csv = download_new_files_as_csv(s3, new_files)

    for new_file in new_files_csv:
        table = create_clickhouse_table(new_file)
        insert_data_to_clickhouse(new_file, table)
        report_df = SQL_requests(table)
        upload_report_to_S3_bucket(s3, report_df, table)
        drop_clickhouse_table(table)

    Variable.set('files_in_bucket', value=current_files)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['minin.kp11@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),

}
with DAG(
    dag_id='S3_ETL_v3',
    default_args=default_args,
    description='The DAG for ETL AWS S3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    max_active_runs=2,
) as dag:

    etl = PythonOperator(
        task_id='check_bucket_n_download_df',
        python_callable=ETL,
        provide_context=True,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )
