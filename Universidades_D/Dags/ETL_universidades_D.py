from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# [END import_module]

# [START default_args]
# These args will get pass on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}


# [END default_args]

# START extract function for get data from Database
def extract():
    pass
    # list of action and process required to this function


# END extract function

#  START transform function for data extracted
def transform():
    pass
    # operations needed to manage data


# END transform function

#  START load function for store data into S3 repository
def load():
    pass
    # link this task to target data storage


# END load function


#  START instantiate_dag
with DAG(
        'ETL_universidades_D',
        default_args=default_args,
        description='ETL DAG for University D data',
        schedule_interval=timedelta(hours=1),
        start_date=datetime.today(),

) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # Execute PythonOperator to the extract function
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
    )

    # Execute PythonOperator to the transform function
    transform_data_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    # Execute PythonOperator to the load function
    load_data_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    extract_data_task >> transform_data_task >> load_data_task
