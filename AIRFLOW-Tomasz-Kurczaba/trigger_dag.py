from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from subdag_factory import pushers_sub_dag
from airflow.operators.subdag import SubDagOperator
from airflow.models import Variable
from slack import WebClient
from slack.errors import SlackApiError
from airflow.hooks.base_hook import BaseHook
from plugins.smart_file_sensors import SmartFileSensor


def send_to_slack():
    """
    This function sends a message to the Slack workspace, which is accessed by the token stored in the slack_token variable as a secret backend using Vault.
    """
    my_var = Variable.get("slack_token")
    print(f'My variable is: {my_var}')
    client = WebClient(token=my_var)
    try:
       response = client.chat_postMessage(
       channel="general",
       text="Hello from your app! :tada:")
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
       assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'

    
def sending_to_slack():
    """
    This function sends a message to the Slack workspace, which is accessed by the token stored in the slack_connection using Airflow UI.
    """
    slack_token = BaseHook.get_connection('slack_connection')
    client = WebClient(token=slack_token.password)
    try:
       response = client.chat_postMessage(
       channel=slack_token.login,
       text="Hello from your app! :tada:")
    except SlackApiError as e:
        # You will get a SlackApiError if "ok" is False
       assert e.response["error"]  # str like 'invalid_auth', 'channel_not_found'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 8, 10),
    'render_template_as_native_obj': True
}

with DAG(
    'trigger_dag', default_args=default_args, schedule_interval = "*/15 * * * *",
) as dag:
    path = "/opt/home/files/" + Variable.get('path', default_var='run.txt')

   
    waitForFile = SmartFileSensor(task_id= "file_sensor_task", poke_interval= 15, filepath=path,fs_conn_id='fs_default')
    trrigerDAG = TriggerDagRunOperator(task_id="TriggerDag",  trigger_dag_id="dag_id_1",wait_for_completion=True)
    removeFile = SubDagOperator(subdag = pushers_sub_dag('trigger_dag', 'SubDag', datetime(2022, 7, 22),"*/1 * * * *"),task_id= "SubDag")
    

    alertSlack = PythonOperator(task_id = "alertToSlack", python_callable=sending_to_slack)
    #alertSlack2 = PythonOperator(task_id = "alertSlack2", dag = dag, python_callable= send_to_slack) using secret backend

    waitForFile >> trrigerDAG >> removeFile >> alertSlack


			