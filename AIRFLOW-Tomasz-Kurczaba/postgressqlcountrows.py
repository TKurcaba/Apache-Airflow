from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
class PostgreSQLCountRows(BaseOperator):
    def __init__(self,mysql_conn_id: str, table_name: str,  **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.mysql_conn_id = mysql_conn_id
        

    def execute(self, context):
        print(os.getcwd())
        hook = PostgresHook(mysql_conn_id=self.mysql_conn_id)
        sql = "SELECT COUNT(*) FROM {};".format(self.table_name)
        result = hook.get_first(sql)
        message = result[0]
        print(message)
        return message
