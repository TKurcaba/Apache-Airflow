from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.models import Variable


def puller(**kwargs):
    ti = kwargs['ti']
    print(ti.xcom_pull(task_ids='query_the_table', key='queryTask', dag_id='dag_id_1'))
    print(kwargs['ti']) 
    
    
def pushers_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):
   dag = DAG(
     '%s.%s' % (parent_dag_name, child_dag_name),
     schedule_interval=schedule_interval,
     start_date=start_date,render_template_as_native_obj = True
   )

   SensorTriggeredDag = ExternalTaskSensor(task_id="sensor_triggered_task",allowed_states=['success'],external_dag_id="dag_id_1", execution_delta=timedelta(minutes=5) , dag=dag)
   PrintResult =  PythonOperator(task_id = "printResult", provide_context = True, dag = dag, python_callable=puller) 
   path = "/opt/home/files/" + Variable.get('path', default_var='run.txt')
   RemoveRunFile = BashOperator(task_id= "RemoveRunFile", bash_command = "rm "+ path, dag = dag)
   CreateTimestamp = BashOperator(task_id= "CreateTimestamp", bash_command = "touch finished_{{ ds_nodash }}",
   dag = dag)
   SensorTriggeredDag >> PrintResult >> RemoveRunFile >> CreateTimestamp
   return dag
