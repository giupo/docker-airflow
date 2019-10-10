"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 10, 10),
    "email": ["giuseppe.acito@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
nodes = {}

dag = DAG("grafo", default_args=default_args, schedule_interval=timedelta(days=1))

with open('/usr/local/airflow/dags/archi.txt') as f:
    linee = [x.strip() for x in f.readlines()]
    archi = [x.split(' ') for x in linee]

for arco in archi:
    n1 = arco[0]
    n2 = arco[1]
    print("add edge " + str(arco))

    if n1 not in nodes:
        nodes[n1] = BashOperator(task_id=n1, bash_command="sleep 1", dag=dag)
    if n2 not in nodes:
        nodes[n2] = BashOperator(task_id=n2, bash_command="sleep 1", dag=dag)

    nodes[n2].set_upstream(nodes[n1])
    globals()[n1] = nodes[n1]
    globals()[n2] = nodes[n2]
