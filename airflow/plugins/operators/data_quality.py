from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks= dq_checks
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table in self.tables:
            for check in self.dq_checks:
                check_sql = check['check_sql'].format(table)
                self.log.info(f"Starting data quality check for {table}")
                records = redshift_hook.get_records(check_sql)
                num_records = records[0][0]
                if num_records < check['expected_result']:
                    raise ValueError("Data quality check failed, sql test {} did not pass expected result {}".format(check_sql, check['expected_result']))
                self.log.info("Data quality on table {} passed test {} against {}".format(table, check_sql, check['expected_result']))
        