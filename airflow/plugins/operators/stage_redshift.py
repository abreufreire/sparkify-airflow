from airflow.hooks.postgres_hook import PostgresHook
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    operator to copy/stage data from S3 to Redshift.
    """

    ui_color = "#358140"

    @apply_defaults
    def __init__(self,
                 s3_bucket,
                 s3_prefix,
                 table,
                 redshift_conn_id="redshift",
                 aws_conn_id="aws_credentials",
                 copy_options="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # map params:
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options

    def execute(self, context):
        #self.log.info("StageToRedshiftOperator...")
        aws_hook = AwsHook(self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info("\ndeleting data in Redshift..")
        redshift_hook.run(f"DELETE FROM {self.table}")

        self.log.info(f"\ncopying data from {self.s3_bucket}/{self.s3_prefix} to {self.table}..")
        copy_query = """COPY {table} FROM 's3://{s3_bucket}/{s3_prefix}' 
                        with credentials 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
                        {copy_options};
                        """.format(table=self.table,
                                   s3_bucket=self.s3_bucket,
                                   s3_prefix=self.s3_prefix,
                                   access_key=credentials.access_key,
                                   secret_key=credentials.secret_key,
                                   copy_options=self.copy_options)

        self.log.info("\ncopying..")
        redshift_hook.run(copy_query)

        self.log.info("\ncopy done.")
