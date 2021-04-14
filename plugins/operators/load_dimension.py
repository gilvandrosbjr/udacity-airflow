from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Operator that is responsible to truncate and load the Dimension table.
    This operator only truncates the dimension table if the flag `truncate_before_load` is activated
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 target_table="",
                 sql="",
                 truncate_before_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.sql = sql
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        self.log.info('LoadDimensionOperator has started')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info(f'Truncating the {self.target_table} table')
            redshift_hook.run(f"TRUNCATE TABLE {self.target_table}")

        query = """INSERT INTO {} {}""".format(self.target_table, self.sql)
        redshift_hook.run(query)

        self.log.info('LoadDimensionOperator has finished')
