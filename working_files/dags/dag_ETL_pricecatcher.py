from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import papermill as pm
from pathlib import Path

default_args = {
    'owner' : 'airflow',
    'start_date' : datetime(2024,4,25),
    'retries' : 1,
    'retry_delay' : timedelta(minutes= 30),
    "depends_on_past" : False
}

dag = DAG("ETL_Price_Catcher_DAG", default_args=default_args, schedule=timedelta(days=1))


def get_folder_name(**kwargs): 
    """
    Note: This is for me to able to change where I code (either in my PC or Docker environment)
    In Docker env follows Linux - e.g /usr/local/airflow//working_files
    In PC follows Windows       - e.g. C:/Users/Azwan/Folder/DOSM/Data/....
    """
    path = Path()
    data_path = str(path.cwd() / 'data')
    print("\DAG FOLDER:", Path().absolute().as_posix().split('working_files')[0] + '/working_files/dags')
    
    
def exec_extract_nbook(**kwargs):
    """
    Execute ETL notebook using Papermill library
    """
    pm.execute_notebook(input_path = "./working_files/dags/nb_pricecatcher.ipynb", 
                        output_path = "./working_files/dags/papermill_logging/nb_pricecatcher.ipynb",
                        parameters= {'date' : '2022-02'})
    
def exec_load_nbook(**kwargs):
    """
    Execute ETL notebook using Papermill library
    """
    pm.execute_notebook(input_path = "./working_files/dags/nb_push_sf.ipynb", 
                        output_path = "./working_files/dags/papermill_logging/nb_push_sf.ipynb",
                        parameters= {'date' : '2022-02'})

print_folder = PythonOperator(dag=dag,
                           task_id='Task_PrintFolder',
                           python_callable=get_folder_name)

extract_from_dosm = PythonOperator(dag=dag,
                           task_id='exec_extract_nbook',
                           python_callable=exec_extract_nbook)

push_to_snowflake = PythonOperator(dag=dag,
                           task_id='exec_load_nbook',
                           python_callable=exec_load_nbook)


print_folder >> extract_from_dosm >> push_to_snowflake