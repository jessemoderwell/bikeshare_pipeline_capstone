from datetime import datetime, timedelta
import os
from airflow import DAG
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.bash import BashOperator
from operators.data_quality import DataQualityOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'catchup': False,
}

dag = DAG('bikeshare_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          catchup=False,
          template_searchpath="/scripts"
        )

JOB_FLOW_OVERRIDES = {
    'Name': 'test-cluster-airflow',
    'ReleaseLabel': 'emr-6.7.0',
    'LogUri': 's3n://airflow-bikeshare/',
    'Applications': [
        {
            'Name': 'Spark'
        },
        {
            'Name': 'Hadoop'
        },
    ],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'MASTER',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'CORE',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        # 'Ec2KeyName': '{{ var.value.emr_ec2_key_pair }}',
    },
    'BootstrapActions': [
        {
            'Name': 'string',
            'ScriptBootstrapAction': {
                'Path': 's3://bootstrap6/bootstrap.sh',
            }
        },
    ],
    # 'Configurations': [
    #     {
    #         'Classification': 'spark-hive-site',
    #         'Properties': {
    #             'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
    #         }
    #     }

    # ],
    'VisibleToAllUsers': True,
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    # 'EbsRootVolumeSize': 32,
    # 'StepConcurrencyLevel': 1,
    # 'Tags': [
    #     {
    #         'Key': 'Environment',
    #         'Value': 'Development'
    #     },
    #     {
    #         'Key': 'Name',
    #         'Value': 'Airflow EMR test Project'
    #     },
    #     {
    #         'Key': 'Owner',
    #         'Value': 'admin'
    #     }
    # ]
}

# create_emr_cluster = EmrCreateJobFlowOperator(
#     task_id="create_emr_cluster",
#     job_flow_overrides=JOB_FLOW_OVERRIDES,
#     aws_conn_id="aws_default",
#     emr_conn_id="emr_default",
#     dag=dag,
# )

# SPARK_STEPS = [ # Note the params values are supplied to the operator
#     {
#         "Name": "Run pyspark script",
#         "ActionOnFailure": "CANCEL_AND_WAIT",
#         "HadoopJarStep": {
#             "Jar": "command-runner.jar",
#             "Args": [
#                 "spark-submit",
#                 "--deploy-mode",
#                 "client",
#                 "s3://bikeshare-script/emr_script.py",
#             ],
#         },
#     },
# ]
    
# step_adder = EmrAddStepsOperator(
#     task_id='add_steps',
#     job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
#     aws_conn_id='aws_default',
#     steps=SPARK_STEPS,
#     dag=dag
#     )

# last_step = len(SPARK_STEPS) - 1

# step_checker = EmrStepSensor(
# task_id='watch_step',
# job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
# step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
# + str(last_step)
# + "] }}",
# aws_conn_id='aws_default',
# dag=dag
# )

# cluster_remover = EmrTerminateJobFlowOperator(
# task_id='remove_cluster',
# job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
# aws_conn_id='aws_default',
# dag=dag
# )

bash_command = "python3 etl.py"

redshift_process_files = BashOperator(
    task_id='redshift_process',
    bash_command=bash_command,
    dag=dag,
    cwd=dag.folder)

#Check that there is data in the table
over_zero_check = {'check_sql': "SELECT COUNT(*) from {}", 
'expected_result': 1}

# Make sure that correct number of records (neighborhood/ vehicle)
# exist in the table
neighborhood_check = {'check_sql': "SELECT count(distinct concat(end_neighborhood, \
vehicle)) from {}", 'expected_result':90}

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tables=["neighborhood_metrics"],
    dq_checks=[over_zero_check, neighborhood_check]
)

# create_emr_cluster >> step_adder
# step_adder >> step_checker
# step_checker >> cluster_remover
# step_checker >> redshift_process_files
redshift_process_files >> run_quality_checks

