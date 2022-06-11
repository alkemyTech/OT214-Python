import os
from datetime import date, datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from sqlalchemy import create_engine

from config.logging_config import logging_configuration

# [END import_module]

# start configured log for notice processes and fails
log = logging_configuration()

# [START default_args]
# These args will get pass on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'alkymer',
    'depends_on_past': False,
    'email': ['juan.i.elizondo@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=5),
    'scheduler_interval': timedelta(hours=1)
}


# [END default_args]
class ETL:

    # credentials for establish a database connection
    @staticmethod
    def __get_connection_credentials():
        try:
            load_dotenv()
        except Exception as e:
            log.error(f'environment file missing or wrong, check file: {e}')
        pg_user = os.getenv('_PG_USER')
        pg_passwd = os.getenv('_PG_PASSWD')
        pg_host = os.getenv('_PG_HOST')
        pg_port = os.getenv('_PG_PORT')
        pg_db = os.getenv('_PG_DB')
        return f'postgresql://{pg_user}:{pg_passwd}@{pg_host}:{pg_port}/{pg_db}'

    # credentials for establish S3 repository connection
    @staticmethod
    def __get_s3_credentials():
        try:
            load_dotenv()
        except Exception as e:
            log.error(f'environment file missing or wrong, check file: {e}')
        s3_credentials = {
            'bucket_name': os.getenv('_bucket_name'),
            'public_key': os.getenv('_public_key'),
            'secret_key': os.getenv('_secret_key')
        }
        return s3_credentials

    # getting engine for plug connection and test status
    @staticmethod
    def __create_connection():
        url = ''
        try:
            log.info('Dag starting extract task operation')
            url = ETL.__get_connection_credentials()
        except Exception as e:
            log.error(f'Connection to database fails, check credentials: {e}')
        engine = create_engine(url)
        return engine

    # get auxiliary csv file of postal codes
    @staticmethod
    def __get_support_file():
        filename = '/files/codigos_postales.csv'
        path_file = os.path.dirname(__file__) + filename
        df_postal_code = pd.read_csv(path_file)
        return df_postal_code

    # calculates age of people from his birth date for two ways informed: yy or YYYY format
    def age(self, birth_d):
        birth_date = datetime.strptime(birth_d, '%Y-%m-%d').date()
        today = date.today()
        if birth_date.year + 18 <= today.year:
            result = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        elif 2000 <= birth_date.year <= today.year:
            result = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        else:
            birth_date_cxx = birth_date.year - 100
            result = today.year - birth_date_cxx - ((today.month, today.day) < (birth_date.month, birth_date.day))
        return result

    # implemented function to modeling data requeried format for utn university
    def normalize_utn(self, df, df_cp):
        unwanted = ['md', 'dds', r'mr.', 'dvm', r'mrs.', r'dr\.', 'iii', r'ms.', 'phd']
        for word in unwanted:
            df['nombre'] = df['nombre'].str.replace(word, '', regex=True)
            df['nombre'] = df['nombre'].str.replace(r'[^\w\s]', '', regex=True)
            df['nombre'] = df['nombre'].str.strip()
        df[['first_name', 'last_name']] = df.pop('nombre').str.split(n=1, pat=' ', expand=True)
        df.pop('direccion')
        df.rename(columns={
            'sexo': 'gender',
            'birth_date': 'age'
        }, inplace=True)
        names = ['university', 'career', 'first_name', 'last_name', 'gender', 'location']
        for column in names:
            df[column] = df[column].str.replace(r'[^\w\s]|_', '', regex=True)
            df[column] = df[column].str.strip()
        gender = {'m': 'male', 'f': 'female'}
        for k in gender.keys():
            df['gender'] = df['gender'].str.replace(k, gender[k], regex=True)
        df['age'] = pd.to_datetime(df['age'], format='%Y/%m/%d')
        df = df.astype({'age': str}, errors='raise')
        ages = []
        for birth_date in df['age']:
            age = ETL.age(self, birth_date)
            ages.append(age)
        df_age = pd.DataFrame({'age': ages})
        df.pop('age')
        df = df.join(df_age)
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%Y/%m/%d')
        df['inscription_date'] = df['inscription_date'].dt.strftime('%Y/%m/%d')
        codes = []
        df_cp['localidad'] = df_cp['localidad'].str.lower()
        dict_codes = pd.Series(df_cp.codigo_postal.values, index=df_cp.localidad).to_dict()
        for location in df['location']:
            code = dict_codes[location]
            codes.append(code)
        df_codes = pd.DataFrame({'postal_code': codes})
        df_codes = df_codes.astype({'postal_code': str}, errors='raise')
        df = df.join(df_codes)
        new_column_names = ['university', 'career', 'inscription_date', 'first_name',
                            'last_name', 'gender', 'age', 'postal_code', 'location', 'email']
        df = df.reindex(columns=list(new_column_names))
        return df

    # implemented function to modeling data requeried format for tdf university
    def normalize_tdf(self, df, df_cp):
        unwanted = ['md', 'dds', r'mr.', 'dvm', r'mrs.', r'dr\.', 'iii', r'ms.', 'phd']
        for word in unwanted:
            df['names'] = df['names'].str.replace(r'[^\w\s]|_', ' ', regex=True)
            df['names'] = df['names'].str.replace(word, '', regex=True)
            df['names'] = df['names'].str.strip()
        df[['first_name', 'last_name']] = df.pop('names').str.split(n=1, pat=' ', expand=True)
        df.pop('direcciones')
        df.rename(columns={
            'sexo': 'gender',
            'birth_dates': 'age',
            'correos_electronicos': 'email',
            'universidad': 'university',
            'fecha_de_inscripcion': 'inscription_date',
            'careers': 'career',
            'codigo_postal': 'postal_code'
        }, inplace=True)
        names = ['university', 'career', 'first_name', 'last_name', 'gender']
        for column in names:
            df[column] = df[column].str.replace(r'[^\w\s]|_', ' ', regex=True)
            df[column] = df[column].str.strip()
        gender = {'m': 'male', 'f': 'female'}
        for k in gender.keys():
            df['gender'] = df['gender'].str.replace(k, gender[k], regex=True)
        df['age'] = pd.to_datetime(df['age'], format='%d/%b/%y')
        df = df.astype({'age': str}, errors='raise')
        ages = []
        for birth_date in df['age']:
            age = ETL.age(self, birth_date)
            ages.append(age)
        df_age = pd.DataFrame({'age': ages})
        df.pop('age')
        df = df.join(df_age)
        df['inscription_date'] = pd.to_datetime(df['inscription_date'], format='%d/%b/%y')
        df['inscription_date'] = df['inscription_date'].dt.strftime('%Y/%m/%d')
        locations = []
        df_cp['localidad'] = df_cp['localidad'].str.lower()
        dict_codes = pd.Series(df_cp.localidad.values, index=df_cp.codigo_postal).to_dict()
        for codigo in df['postal_code']:
            location = dict_codes[codigo]
            locations.append(location)
        df_codes = pd.DataFrame({'location': locations})
        df_codes = df_codes.astype({'location': str}, errors='raise')
        df = df.join(df_codes)
        new_column_names = ['university', 'career', 'inscription_date', 'first_name',
                            'last_name', 'gender', 'age', 'postal_code', 'location', 'email']
        df = df.reindex(columns=list(new_column_names))
        return df

    # collect the data from database into dataframes and then store to csv files
    def data_to_csv(self, connection, fn_utn, fn_tdf, sql1, sql2):
        df_utn, df_tdf = '', ''
        try:
            df_utn = pd.read_sql(sql1, con=connection)
            df_tdf = pd.read_sql(sql2, con=connection)
        except Exception as e:
            log.error(f'Pandas fails to read sql query: {e}')
        path_directory = os.path.dirname(__file__) + '/files'
        try:
            os.makedirs(path_directory, exist_ok=True)
            log.info(f'Directory {"files"} created successfully')
        except OSError as error:
            log.error(f'Directory {"files"} can not be created: {error}')
        path_utn = path_directory + '/' + fn_utn
        path_tdf = path_directory + '/' + fn_tdf
        try:
            df_utn.to_csv(path_utn, index=False)
            df_tdf.to_csv(path_tdf, index=False)
            log.info('data files csv created successfully')
        except Exception as e:
            log.error(f'Fails to create csv data files: {e}')

    # START extract function for get data from Database
    def extract(self, filename_utn, filename_tdf, **kwargs):
        connection = self.__create_connection()
        connection_status = connection.connect()
        connection_check = bool(connection_status)
        if connection_check:
            log.info('Connection to database successful')
        else:
            log.error('Connection to database fails')
        query_utn = kwargs['templates_dict']['query_utn']
        query_tdf = kwargs['templates_dict']['query_tdf']
        try:
            ETL.data_to_csv(self, connection, filename_utn, filename_tdf, query_utn, query_tdf)
            log.info('Dag extract task completed')
        except Exception as e:
            log.critical(f'database error runtime query passed: {e}')

    # END extract function

    #  START transform function for data extracted
    def transform(self, filename_utn, filename_tdf):
        df_utn, df_tdf, df_utn_raw, df_tdf_raw = '', '', '', ''
        log.info('Dag starting transform task operation')
        path_files = os.path.dirname(__file__) + '/files/'
        path_utn = path_files + filename_utn
        path_tdf = path_files + filename_tdf
        try:
            df_utn_raw = pd.read_csv(path_utn)
            df_tdf_raw = pd.read_csv(path_tdf)
        except Exception as e:
            log.error(f'csv files not found or corrupted: {e}')
        df_postal_code = ETL.__get_support_file()
        df_utn = ETL.normalize_utn(self, df_utn_raw, df_postal_code)
        df_tdf = ETL.normalize_tdf(self, df_tdf_raw, df_postal_code)
        path_txt_utn = path_files + 'universidad_tecnologica_nacional.txt'
        path_txt_tdf = path_files + 'universidad_tres_de_febrero.txt'
        try:
            df_utn.to_csv(path_txt_utn, index=False, sep='\t', encoding='utf8')
            df_tdf.to_csv(path_txt_tdf, index=False, sep='\t', encoding='utf8')
            log.info('"txt" files created successufully in "files" directory')
            log.info('Dag transform task completed')
        except Exception as e:
            log.error(f'fails to save data extrated to "txt" files: {e}')

    # END transform function

    #  START load function for store data into S3 repository

    def upload_data_utn(self, filename_utn):
        s3_connect = ETL.__get_s3_credentials()
        log.info('upload to S3 task starting...')
        client_utn = boto3.client('s3',
                                  aws_access_key_id=s3_connect['public_key'],
                                  aws_secret_access_key=s3_connect['secret_key'])
        path_txt_utn = os.path.dirname(__file__) + '/files/' + filename_utn
        try:
            with open(path_txt_utn, 'rb') as file:
                client_utn.upload_fileobj(file, s3_connect['bucket_name'], filename_utn)
        except Exception as e:
            log.error(f'file {filename_utn} not found or directory is missing: {e}')
        s3_session = boto3.Session(
            aws_access_key_id=s3_connect['public_key'],
            aws_secret_access_key=s3_connect['secret_key'])
        s3 = s3_session.resource('s3')
        bucket = s3.Bucket(s3_connect['bucket_name'])
        for _ in bucket.objets.all():
            if filename_utn in bucket.key:
                log.info(f'upload of {filename_utn} file to S3 repository completed')

    def upload_data_tdf(self, filename_tdf):
        s3_connect = ETL.__get_s3_credentials()
        log.info('upload to S3 task starting...')
        client_utn = boto3.client('s3',
                                  aws_access_key_id=s3_connect['public_key'],
                                  aws_secret_access_key=s3_connect['secret_key'])
        path_txt_tdf = os.path.dirname(__file__) + '/files/' + filename_tdf
        try:
            with open(path_txt_tdf, 'rb') as file:
                client_utn.upload_fileobj(file, s3_connect['bucket_name'], filename_tdf)
        except Exception as e:
            log.error(f'file {filename_tdf} not found or directory is missing: {e}')
        s3_session = boto3.Session(
            aws_access_key_id=s3_connect['public_key'],
            aws_secret_access_key=s3_connect['secret_key'])
        s3 = s3_session.resource('s3')
        bucket = s3.Bucket(s3_connect['bucket_name'])
        for _ in bucket.objets.all():
            if filename_tdf in bucket.key:
                log.info(f'upload of {filename_tdf} file to S3 repository completed')

    # END load function

    #  START instantiate_dag


path = os.path.dirname(__file__)
path_sql = path.replace(r'dags', r'sql')

with DAG(
        'ETL_Universities_D',
        default_args=default_args,
        description='ETL DAG for University D data',
        schedule_interval=timedelta(hours=1),
        start_date=datetime(2022, 1, 1),
        template_searchpath=[path_sql]

) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # create object ETL for process
    ETL_DAG = ETL()

    # Execute PythonOperator to the extract function
    extract_data_task = PythonOperator(
        task_id='extract_data',
        op_kwargs={'filename_utn': 'universidad_tecnologica_nacional.csv',
                   'filename_tdf': 'universidad_tres_de_febrero.csv'},
        templates_dict={'query_utn': 'utn.sql',
                        'query_tdf': 'tres_de_febrero.sql'},
        templates_exts=['.sql', ],
        python_callable=ETL_DAG.extract,
        dag=dag
    )

    # Execute PythonOperator to the transform function
    transform_data_task = PythonOperator(
        task_id='transform',
        op_kwargs={'filename_utn': 'universidad_tecnologica_nacional.csv',
                   'filename_tdf': 'universidad_tres_de_febrero.csv'},
        python_callable=ETL_DAG.transform,
        dag=dag
    )

    # Execute PythonOperator to upload UTN data to S3 Repository
    load_data_utn = PythonOperator(
        task_id='upload_data_utn',
        op_kwargs={'filename_utn': 'universidad_tecnologica_nacional.txt'},
        python_callable=ETL_DAG.upload_data_utn,
        dag=dag
    )

    # Execute PythonOperator to upload TDF data to S3 Repository
    load_data_tdf = PythonOperator(
        task_id='upload_data_tdf',
        op_kwargs={'filename_tdf': 'universidad_tecnologica_nacional.txt'},
        python_callable=ETL_DAG.upload_data_tdf,
        dag=dag
    )

    extract_data_task >> transform_data_task >> [load_data_utn, load_data_tdf]
