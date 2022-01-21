#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    operator to create dimension table in Redshift.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 select_sql="",
                 mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # map params:
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        # self.log.info("LoadDimensionOperator..")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.mode == "truncate":
            self.log.info(f"\ndeleting data from {self.table}..")
            del_sql = f"DELETE FROM {self.table};"
            redshift_hook.run(del_sql)
            self.log.info("\ndelete done.")

        sql = """INSERT INTO {table} {select_sql};""".format(table=self.table, select_sql=self.select_sql)

        self.log.info(f"loading data into {self.table}..")
        redshift_hook.run(sql)
        self.log.info("load done.")
