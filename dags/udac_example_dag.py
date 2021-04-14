from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from plugins.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from plugins.helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'email': ['gilvandrosbjr@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'sla': timedelta(hours=1)
}

dag = DAG('airflow_etl_sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# template field to backfill
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    target_table="public.staging_events",
    s3_path="s3://udacity-dend/log_data",  # Variable.get("s3_log_data_path")
    aws_region=Variable.get("aws_region", "us-west-2"),
    copy_options="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
    provide_context=True,
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id="redshift",
    aws_conn_id="aws_credentials",
    target_table="public.staging_songs",
    s3_path="s3://udacity-dend/song_data",  # Variable.get("s3_song_data_path")
    aws_region=Variable.get("aws_region", "us-west-2"),
    provide_context=True,
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id="redshift",
    target_table="public.songplays",
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id="redshift",
    target_table="public.users",
    sql=SqlQueries.user_table_insert,
    truncate_before_load=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id="redshift",
    target_table="public.songs",
    sql=SqlQueries.song_table_insert,
    truncate_before_load=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id="redshift",
    target_table="public.artists",
    sql=SqlQueries.artist_table_insert,
    truncate_before_load=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id="redshift",
    target_table="public.time",
    sql=SqlQueries.time_table_insert,
    truncate_before_load=True,
    dag=dag
)

# TODO: Move to a different place
data_quality_check_params = [
    {
        'sql': 'SELECT COUNT(1) FROM public.staging_events',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.staging_songs',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.songplays',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.users',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.songs',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.artists',
        'expected_count': 1
    },
    {
        'sql': 'SELECT COUNT(1) FROM public.time',
        'expected_count': 1
    }
]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id="redshift",
    data_quality_check_params=data_quality_check_params,
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Defining the task dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
