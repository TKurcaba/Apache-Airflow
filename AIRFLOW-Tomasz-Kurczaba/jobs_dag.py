from datetime import date,datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash import BashOperator
import logging
from json import dumps
from airflow.hooks.postgres_hook import PostgresHook
import uuid
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from postgressqlcountrows import PostgreSQLCountRows

config = {
    'dag_id_1':{'schedule_interval': "*/10 * * * *", "start_date": datetime(2022, 7, 22), "table_name": "table1"},  
    'dag_id_2':{'schedule_interval': "*/2 * * * *", "start_date": datetime(2022, 7, 22), "table_name": "table_name_2"},  
    'dag_id_3':{'schedule_interval': "@daily", "start_date": datetime(2022, 7, 22), "table_name": "table_name_3"}}

def loggingInformation(logger, task_id, database, **kwargs):
    info = "{} start processing tables in database: {}"
    logger.info(info.format(task_id, database))	
    kwargs['ti'].xcom_push(key="queryTask", value=kwargs['run_id']+" ended")
    kwargs['ti'].xcom_push(key="ExectutionDate", value = dumps(kwargs['execution_date'], default=json_serial))
def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

def check_table_exist( sql_to_check_table_exist, table_name):
    hook = PostgresHook()
    query = hook.get_records(sql="SELECT * FROM pg_tables;")
    for result in query:
      if 'airflow' in result:
         schema = result[0]
         print(schema)
         break

    query = hook.get_first(sql = sql_to_check_table_exist.format( table_name, schema))
    print(query)
    """ method to check that table exists """
    if query:
       return 'insert_new_row'
    else:
       return 'create_the_table'

def push(**kwargs):
    pg_hook = PostgresHook.get_hook("postgres_default")
    connection = pg_hook.get_conn() 
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM dag2;")
    sources = cursor.fetchall()
    print(sources)	
    kwargs['ti'].xcom_push(key="queryTask", value=kwargs['run_id']+" ended")
    kwargs['ti'].xcom_push(key="ExectutionDate", value = dumps(kwargs['execution_date'], default=json_serial))



logger = logging.getLogger("airflow.task")
for row in config:
    my_dag= DAG(
	row, start_date=config.get(row).get("start_date"),
    	schedule_interval=config.get(row).get("schedule_interval"),catchup=False, render_template_as_native_obj = True )
    
    start = PythonOperator(task_id="start_process",python_callable=loggingInformation,provide_context = True, queue="jobs_queue", op_kwargs={'logger':logger,'task_id':row, 'database': config.get(row).get("table_name")}, dag=my_dag)
    
    get_current_name = BashOperator(task_id="get_current_name", bash_command="echo $(whoami)", do_xcom_push = True ,queue="jobs_queue",  dag=my_dag)
    
    fork = BranchPythonOperator(    task_id='check_table_exist',queue="jobs_queue", python_callable=check_table_exist,op_args = ["SELECT * FROM information_schema.tables WHERE table_name = '{}' AND table_schema = '{}';", config.get(row).get("table_name")], dag=my_dag)
    
    insert_new_row = PostgresOperator(task_id="insert_new_row", queue="jobs_queue", dag= my_dag, trigger_rule = "none_failed", sql = "INSERT INTO '%(table)s' VALUES(%(random)s, '{{ ti.xcom_pull(task_ids='get_current_name', key='return_value') }}', %(date)s);", 
             parameters = {"table": config.get(row).get("table_name"),"random": uuid.uuid4().int % 123456789, "date": datetime.now()})
    
    create_the_table = PostgresOperator(task_id="create_the_table", dag= my_dag, queue="jobs_queue", sql = "CREATE TABLE {}(custom_id integer NOT NULL, user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);".format(config.get(row).get("table_name")))
     
    #query = PythonOperator(task_id="query_the_table", dag= my_dag, provide_context=True, python_callable = push)
    query = PostgreSQLCountRows(task_id = "query_the_table", dag = my_dag,queue="jobs_queue",  mysql_conn_id = "postgres_default", table_name=config.get(row).get("table_name"))
    start.set_downstream(get_current_name)
    get_current_name.set_downstream(fork)
    fork.set_downstream(create_the_table)
    fork.set_downstream(insert_new_row)
    create_the_table.set_downstream(insert_new_row)
    insert_new_row.set_downstream(query)
    globals()[row] = my_dag
