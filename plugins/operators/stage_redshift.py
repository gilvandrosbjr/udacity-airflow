from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Operator that is responsible to build the Redshift COPY command with different options and load the data
    from S3 to the target table.
    """

    ui_color = '#358140'

    template_fields = ('s3_path',)

    copy_command = """
        COPY {} FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 target_table="",
                 s3_path="",
                 aws_region="",
                 copy_options="JSON 'auto'",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.target_table = target_table
        self.s3_path = s3_path
        self.aws_region = aws_region
        self.copy_options = copy_options

    def execute(self, context):
        self.log.info('StageToRedshiftOperator has started')

        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_conn_id)
        aws_credentials = aws_hook.get_credentials(self.aws_region)

        rendered_s3_path = self.s3_path.format(**context)

        query = self.copy_command.format(self.target_table, rendered_s3_path, aws_credentials.access_key,
                                         aws_credentials.secret_key, self.aws_region, self.copy_options)

        redshift_hook.run(query)

        self.log.info('StageToRedshiftOperator has finished')
