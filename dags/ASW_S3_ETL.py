"""
Дипломный проект Бабенко Александр
"""
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable

from airflow.operators.python import PythonOperator
import os
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import zipfile
import logging

import pandahouse as ph
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
client = Client('localhost')


def read_config():
    """
    Чтение учетных данных из config.py
    """
    try:
        import config

        # Подключение Clickhouse
        db_name = config.DB_NAME
        host = config.HOST
        user = config.USER
        password = config.PASSWORD

        # Сегменты AWS S3
        from_bucket = config.FROM_BUCKET
        to_bucket = config.TO_BUCKET

        # Ключи AWS
        access_key = config.ACCESS_KEY
        secret_key = config.SECRET_KEY

        return db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key
    except ImportError:
        logging.warning("config.py import error")


def read_old_files_names_csv():
    """
    Получение списка файлов, которые уже были объединены, чтобы избежать повторений.
    """
    with open('old_files.csv', 'r') as file_to_read:
        rows = csv.reader(file_to_read)
        old_files_raw = list(rows)  # Может содержать 0 или более элементов
        if len(old_files_raw) > 0:
            old_files = old_files_raw[0]
        else:
            old_files = []
        return old_files


def write_old_files_names_csv(string_to_write):
    """
    Добавление новых имен в файл csv со списком имен
    """
    with open('old_files.csv', 'a') as file_to_write:
        final_string_to_write = str(string_to_write) + ','
        file_to_write.write(final_string_to_write)
        logging.info(f'File name was add to old_files.csv as {final_string_to_write}')


def get_files_list_in_bucket(s3, from_bucket):
    """
    Получение текущего списка файлов в корзине S3.
    """
    current_files = []
    logging.info('The list of files in the bucket today: ')
    for key in s3.list_objects(Bucket=from_bucket)['Contents']:
        current_files.append(key['Key'])
        logging.info(key['Key'])
    return current_files


def search_for_new_files(current_files):
    """
    Сравнение текущего списка файлов в корзине с нашим старым списком, чтобы найти все новые файлы.
    """
    old_files = read_old_files_names_csv()
    new_files = []
    logging.info('The list of the new files: ')
    for file in current_files:
        if file not in old_files:
            new_files.append(file)
            logging.info(file)
    return new_files


def download_new_files_as_csv(s3, new_files, from_bucket):
    """
    Скачивание .zip архивы новых файлов в корзине и разархивирование .csv с данными;
    """

    new_files_csv = []
    for new_file in new_files:
        s3.download_file(Bucket=from_bucket,
                         Key=new_file,
                         Filename='archive.zip')

        # Добавление имя файла в old_files.csv
        write_old_files_names_csv(new_file)

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


def create_clickhouse_table():
    """
    Функция создает новую таблицу для отчетов.
    """
    table_name = 'trips'
    request = 'CREATE TABLE IF NOT EXISTS tripDB.' + table_name +' (\
               date Date, tripduration Int32, gender Int16)\
               ENGINE = MergeTree ORDER BY date SETTINGS index_granularity = 8192 ;'
    client.execute(request)
    logging.info(f'Clickhouse table created with name: {table_name}')
    return table_name


def get_and_load_clickhouse_reports(db_name, table, host, user, password, s3, to_bucket):
    """
    Найти файлы .csv в каталоге ./datafiles,
    сделать кадр данных для каждого файла,
    загрузить необходимые столбцы в таблицу Clickhouse,
    получать отчеты по SQL запросам,
    загрузить отчеты в целевую корзину;
    """
    for filename in os.listdir('./datafiles'):
        if filename.endswith('.csv'):
            file_link = os.path.join('./datafiles', filename)
            report_month = filename[:7]
            logging.info(f'Found a file for month: {report_month}')
            # Создать фрейм данных из исходного .csv
            df = pd.read_csv(file_link)
            logging.info('initial data frame created')

            # Создать таблицу для экспорта в Clickhouse
            df['date'] = pd.to_datetime(df['starttime']).dt.floor('d')
            df_for_cklickhouse = df[['date', 'tripduration', 'gender']].copy()
            logging.info('data frame created')

            # Вставить данные в Clickhouse
            connection = dict(database=db_name,
                              host=host,
                              user=user,
                              password=password)
            ph.to_clickhouse(df_for_cklickhouse, table, index=False, chunksize=100000, connection=connection)
            message = 'Data Frame loaded to Clickhouse table: ' + table
            logging.info(message)

            report_df = SQL_requests(table)
            upload_report_to_S3_bucket(s3, report_df, report_month, to_bucket)

            # Удалить объединенный файл
            os.remove('./datafiles/' + filename)

    return 'Success!!'


def SQL_requests(table):
    """
    Получение отчетов от Clickhouse через SQL-запрос. Сбор выходных данных в pandas DF.
    После получения отчетов таблица очищается с помощью TRUNCATE.
    """
    # Получение количества поездок в день
    trips_count_list = client.execute(
        'SELECT COUNT(date), date \
        FROM tripDB.' + table + ' \
        GROUP BY date;')
    trips_count_df = pd.DataFrame.from_records(trips_count_list, columns=['trips', 'date'])
    logging.info('number of trips found')

    # Получение продолжительности поездок в днях
    trips_duration_list = client.execute(
        'SELECT ROUND(AVG(tripduration), 3), date \
        FROM tripDB.' + table + ' \
        GROUP BY date;')
    trips_duration_df = pd.DataFrame.from_records(trips_duration_list, columns=['duration', 'data'])
    logging.info('duration found')

    # Получение распределения по полу
    gender_0_list = client.execute(
        'SELECT COUNT(gender), date \
        FROM tripDB.' + table + ' \
        WHERE gender=0 \
        GROUP BY date ;')
    gender_1_list = client.execute(
        'SELECT COUNT(gender), date \
        FROM tripDB.' + table + ' \
        WHERE gender=1 \
        GROUP BY date ;')
    gender_2_list = client.execute(
        'SELECT COUNT(gender), date \
        FROM tripDB.' + table + ' \
        WHERE gender=2 \
        GROUP BY date ;')

    gender_0_df = pd.DataFrame.from_records(gender_0_list, columns=['gender_0', 'data'])
    gender_1_df = pd.DataFrame.from_records(gender_1_list, columns=['gender_1', 'data'])
    gender_2_df = pd.DataFrame.from_records(gender_2_list, columns=['gender_2', 'data'])
    logging.info('gender distribution found')

    # Выполнение расчета процентов
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

    client.execute(
        'TRUNCATE TABLE IF EXISTS tripDB.' + table + ';'
    )

    return report_df


def upload_report_to_S3_bucket(s3, report_df, report_month, to_bucket):
    """
    Сохраняет Pandas DataFrame как файл .CSV и загружает файл в целевую корзину AWS S3.
    """
    file_name = './reports/report_' + report_month + '.csv'
    report_df.to_csv(file_name)
    message = 'Report saved locally as ' + file_name
    logging.info(message)

    key = 'report_' + report_month + '.csv'
    try:
        response = s3.upload_file(file_name, to_bucket, key)
        logging.info(f'File uploaded to the AWS S3 bucket with response: {response}')
    except ClientError as e:
        logging.warning(e)
    except FileNotFoundError as e:
        logging.warning(e)


def collect_new_files():
    """
    Первая из двух основных функций. Предназначен для выполнения первой части процесса ETL:
    чтобы загрузить и разархивировать все новые файлы из первого ведра.
    """
    logging.info('Starting the task')

    # Получение ключей для корзин AWS S3
    db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key = read_config()

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    current_files = get_files_list_in_bucket(s3, from_bucket)
    logging.info(f'Current files in bucket: {current_files}')
    new_files = search_for_new_files(current_files)
    new_files_csv = download_new_files_as_csv(s3, new_files, from_bucket)

    return new_files_csv


def aggregate_and_upload_csv():
    """
    Вторая основная часть процесса ELT. Объединение каждого загруженного файла .CSV, загрузка их в таблицу Clickhouse,
    подготовка отчетов и загрузка отчетов в виде файлов .CSV во вторую корзину AWS S3.
    """
    db_name, host, user, password, from_bucket, to_bucket, access_key, secret_key = read_config()
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    logging.info('AWS S3 connection established')

    table_name = create_clickhouse_table()
    get_and_load_clickhouse_reports(db_name=db_name,
                                    table=table_name,
                                    host=host,
                                    user=user,
                                    password=password,
                                    s3=s3,
                                    to_bucket=to_bucket)


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

    download_files = PythonOperator(
        task_id='download_files',
        python_callable=collect_new_files,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )

    get_and_load_reports = PythonOperator(
        task_id='get_and_load_reports',
        python_callable=aggregate_and_upload_csv,
        op_kwargs={"dag_run_id": "{{ run_id }}"},
        dag=dag,
    )

    download_files >> get_and_load_reports

