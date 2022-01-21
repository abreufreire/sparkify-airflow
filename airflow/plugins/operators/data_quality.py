#!/usr/bin/env python
# -*- coding: utf-8 -*-

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    operator to do quality check on data in production.
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # map params
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info("processing DataQualityOperator...")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        error_count = 0
        failing_checks = []

        for check in self.dq_checks:
            sql = check.get("check_sql")
            exp_res = check.get("expected_result")

            records_query = redshift_hook.get_records(sql)[0]

            if exp_res != records_query[0]:
                error_count += 1
                failing_checks.append(sql)

            if error_count > 0:
                self.log.info("check failed")
                self.log.info(failing_checks)
                raise ValueError("data validation failed")

            self.log.info(f"data validation completed successfully")

        # validation test for production tables
        # for table in self.tables:
        #
        #     self.log.info(f"running validation - table: {table}")
        #     records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table};")
        #
        #     if len(records) < 1 or len(records[0]) < 1:
        #         self.log.error(f"validation error - table: {self.table}")
        #         raise ValueError(f"validation error - table: {self.table}")
        #         n_records = records[0][0]
        #
        #     if n_records < 1:
        #         raise ValueError(f"validation error - table: {self.table}")
