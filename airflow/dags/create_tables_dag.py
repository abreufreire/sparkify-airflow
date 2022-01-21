#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from helpers.create_tables import CreateTables

from datetime import datetime, timedelta
import os

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# settings to all tasks
default_args = {
    "owner": "abreufreire",
    "start_date": datetime(2018, 11, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    'email_on_retry': False,
    "catchup": False
}

# DAG context manager
with DAG("create_tables_dag",
         default_args=default_args,
         description="create tables in Redshift with Airflow",
         schedule_interval=None,
         max_active_runs=1
         ) as dag:

    start_operator = DummyOperator(task_id="begin_execution", dag=dag)

    create_table_staging_events = PostgresOperator(
        task_id="create_table_staging_events",
        dag=dag,
        postgres_conn_id='redshift',
        sql=CreateTables.staging_events_table_create
    )

    create_table_staging_songs = PostgresOperator(
        task_id="create_table_staging_songs",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CreateTables.staging_songs_table_create
    )

    create_table_songplays = PostgresOperator(
        task_id="create_table_songplays",
        dag=dag,
        postgres_conn_id='redshift',
        sql=CreateTables.songplays_table_create
    )

    create_table_artists = PostgresOperator(
        task_id="create_table_artists",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CreateTables.artists_table_create
    )

    create_table_songs = PostgresOperator(
        task_id="create_table_songs",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CreateTables.songs_table_create
    )

    create_table_users = PostgresOperator(
        task_id="create_table_users",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CreateTables.users_table_create
    )

    create_table_time = PostgresOperator(
        task_id="create_table_time",
        dag=dag,
        postgres_conn_id="redshift",
        sql=CreateTables.time_table_create
    )

    schema_created = DummyOperator(task_id="schema_created", dag=dag)

    end_operator = DummyOperator(task_id="stop_execution", dag=dag)

    # DAG dependencies
    start_operator >> [create_table_staging_songs, create_table_staging_events, create_table_songplays, create_table_artists, create_table_songs, create_table_users, create_table_time]

    [create_table_staging_songs, create_table_staging_events, create_table_songplays, create_table_artists, create_table_songs, create_table_users, create_table_time] >> schema_created

    schema_created >> end_operator
