#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import LoadFactOperator
from airflow.operators import LoadDimensionOperator
from airflow.operators import DataQualityOperator

from helpers import SqlQueries

from datetime import datetime, timedelta
import os

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# settings to all tasks
default_args = {
    "owner": "abreufreire",
    "start_date": datetime(2018, 11, 1),
    "depends_on_past": False,
    "email_on_retry": False,
    "email_on_failure": False,
    "catchup": False,  # no historical runs
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

# DAG context manager
with DAG("etl_dag",
         default_args=default_args,
         description="load and transform data in Redshift with Airflow",
         schedule_interval="0 * * * *",  # @daily: 0 0 * * * | @hourly: "0 * * * *" | https://crontab.guru
         catchup=False,
         max_active_runs=1
         ) as dag:

    start_operator = DummyOperator(task_id="begin_execution", dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="stage_events",
        dag=dag,
        s3_bucket="udacity-dend",
        s3_prefix="log_data",
        table="staging_events",
        copy_options="JSON 's3://udacity-dend/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="stage_songs",
        dag=dag,
        s3_bucket="udacity-dend",
        s3_prefix="song_data/A/A/A",
        table="staging_songs",
        copy_options="FORMAT AS JSON 'auto'"
    )

    load_table_songplays = LoadFactOperator(
        task_id="load_fact_table_songplays",
        dag=dag,
        table="songplays",
        select_sql=SqlQueries.songplays_table_insert
    )

    load_dim_table_users = LoadDimensionOperator(
        task_id="load_dim_table_users",
        dag=dag,
        table="users",
        select_sql=SqlQueries.users_table_insert,
        mode="truncate"
    )

    load_dim_table_songs = LoadDimensionOperator(
        task_id="load_dim_table_songs",
        dag=dag,
        table="songs",
        select_sql=SqlQueries.songs_table_insert,
        mode="truncate"
    )

    load_dim_table_artists = LoadDimensionOperator(
        task_id="load_dim_table_artists",
        dag=dag,
        table="artists",
        select_sql=SqlQueries.artists_table_insert,
        mode="truncate"
    )

    load_dim_table_time = LoadDimensionOperator(
        task_id="load_dim_table_time",
        dag=dag,
        table="time",
        select_sql=SqlQueries.time_table_insert,
        mode="truncate"
    )

    run_quality_checks = DataQualityOperator(
        task_id="run_data_quality_checks",
        dag=dag,
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"],
        dq_checks=[
            {"check_sql": "SELECT COUNT(*) FROM users WHERE userid is null", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM songs WHERE songid is null", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM artists WHERE artistid is null", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM time WHERE start_time is null", "expected_result": 0},
            {"check_sql": "SELECT COUNT(*) FROM songplays WHERE playid is null", "expected_result": 0}
        ]
    )

    end_operator = DummyOperator(task_id="stop_execution", dag=dag)

    # DAG dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_table_songplays
    load_table_songplays >> [load_dim_table_songs, load_dim_table_users, load_dim_table_artists, load_dim_table_time]
    [load_dim_table_songs, load_dim_table_users, load_dim_table_artists, load_dim_table_time] >> run_quality_checks
    run_quality_checks >> end_operator
