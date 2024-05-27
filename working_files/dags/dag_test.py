"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# These arguments will be used to define the DAG pipeline
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# We create the pipeline
dag = DAG("tutorial", default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
# First task - using Bash operator to run a command "date"
t1 = BashOperator(task_id="print_date", bash_command="date", dag=dag)
# Second task - using Bash operator to do nothing for 5 seconds
t2 = BashOperator(task_id="sleep", bash_command="sleep 5", retries=3, dag=dag)

# Third task - using Bash operator to print out a parameter
templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id="templated",
    bash_command=templated_command,
    params={"my_param": "Parameter I passed in"},
    dag=dag,
)

# Finally we set dependencies how the task are being run
t2.set_upstream(t1)
t3.set_upstream(t1)

# Or using this method
# t1 >> [t2, t3]
