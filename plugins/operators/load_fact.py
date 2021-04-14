from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Operator that is responsible to truncate and load the Fact table.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql

    def execute(self, context):
        self.log.info('LoadFactOperator has started')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        query = """INSERT INTO {} {}""".format(self.target_table, self.sql)

        redshift_hook.run(query)

        self.log.info('LoadFactOperator has finished')
