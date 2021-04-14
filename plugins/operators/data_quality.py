from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Operator that receives all the Data Quality Queries and the expected results and
    is responsible to run the queries and check the outcome.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 data_quality_check_params=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_check_params = data_quality_check_params

    def execute(self, context):
        self.log.info('DataQualityOperator has started')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for quality_check in self.data_quality_check_params:

            sql = quality_check['sql']
            expected_count = quality_check['expected_count']

            records = redshift_hook.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql} returned no results")
            num_records = records[0][0]
            if num_records < expected_count:
                raise ValueError(f"Data quality check failed. {sql} returned {num_records}, "
                                 f"we expected {expected_count} rows")
            self.log.info(f"Data quality on for the query {sql} check passed with {records[0][0]} records")

        self.log.info('DataQualityOperator has finished')

