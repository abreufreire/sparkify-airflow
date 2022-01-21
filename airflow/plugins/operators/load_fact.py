from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    operator to create fact table in Redshift.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # map params:
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql

    def execute(self, context):
        # self.log.info("LoadFactOperator..")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info(f"\nloading data into {self.table}..")

        sql = """INSERT INTO {table} {select_sql};""".format(table=self.table, select_sql=self.select_sql)

        redshift_hook.run(sql)

        self.log.info("\nload done.")
