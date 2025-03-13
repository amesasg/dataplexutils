from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.utils.dates import days_ago
import random
import logging
from google.cloud import storage
import json
import sys
from airflow.utils.edgemodifier import Label

logger = logging.getLogger(__name__)  # Use __name__ for module-specific logging


from dataplexutils.metadata import Client
from dataplexutils.metadata import ClientOptions

client_options = ClientOptions(
            use_lineage_tables=True,
            use_lineage_processes=True,
            use_profile=True,
            use_data_quality=True        )

location ="us-central1"
project_id = "garciaam-670-20240123092619"
dataset_name = "customer_data_product"
table_name = "customer_data"
client = Client(
            project_id=project_id,
            llm_location=location,
            dataplex_location=location,
            client_options=client_options,
        )

contract_validation_reports_bucket = "dataplex-utils"

# Dummy functions for demonstration purposes
def get_data_product_status(table):
    print("Getting product status from aspect")
    status = client._data_product_ops.get_product_status(table)
    print (status)
    return status

def create_documentation(**context):
    logger.info("Creating documentation...")
    table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
    description = client.generate_table_description(table)
    logger.debug(f"Documentation created {description}")

def attach_mandatory_contract_tags(**context):
    """Simulates attaching mandatory tags."""
    print("Attaching mandatory tags...")
    table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
    contract = context['ti'].xcom_pull(task_ids='create_contract_aspect_types', key='conract_json')
    #client.create_contract_aspects_types(contract,table)
    client.initialize_contract_aspect_product(contract,table) 

def mark_to_review(**context):
    """Simulates attaching mandatory tags."""
    
    logger.info("Update product status to to-review")
    table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
    description = client.update_product_status(table, "to-be-reviewed")


def create_contract_aspect_types(**context):
    table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
    contract = client._data_product_ops.get_contract(table)
    logger.info(f"Using contract {contract}")
    client.create_contract_aspects_types(contract,table)
    logger.info(f"Apect types to support contract created {contract}")
    context['ti'].xcom_push(key='conract_json', value=contract)


def validate_contract(**context):
    table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
    contract = client._data_product_ops.get_contract(table)
    logger.info(f"validating contract for table  : {table} based on json contract {contract}")
    report = client.validate_contract_aspects(contract,table)
    logger.info(f"Validation report : {report}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(contract_validation_reports_bucket)
    file_name = table.replace('.', '-')
    destination_blob_name= f"report/contract_validation_report { file_name}.json"
    blob = bucket.blob(destination_blob_name)
    content_type = "application/json"

    blob.upload_from_string(report, content_type=content_type)
    logger.info("Validation report uplaoded to  gs://{bucket}/{destination_blob_name} ")
    return "invalid_product"

    data = json.loads(report)
    if "field_validation" in json_data:
        for item in json_data["field_validation"]:
            if "value" in item and item["value"] == "false":
                return "invalid_product"
    return "share_data_product"


def validate_data_product_status_and_process(**context):
    """Validates the data product status and triggers appropriate tasks."""
    project_id = "garciaam-670-20240123092619"
    table= f"{project_id}.merchants_data_product.core_merchants"
    context['ti'].xcom_push(key='table', value=table)
    logger.info(f"Validating table {table}")
    
    
def waitting_manual_validation(**context):
    logger.info("Waitting for manual validation and status to be updated to 'to-be-publushed'")
 
def invalid_product(**context):
    logger.info("Error product not valid send email to owner! ")
 

def share_data_product(**context):
    logger.info("TODO update IAM tag")
    data_product_status = get_data_product_status()
    print(f"Data product status: {data_product_status}")
    context['ti'].xcom_push(key='data_product_status', value=data_product_status)

def branch_tasks(**context):
        """Branches the pipeline based on the data product status."""
        table = context['ti'].xcom_pull(task_ids='validate_status', key='table')
        logger.info(table)
        print (table)

        data_product_status = get_data_product_status(table)
        logger.info(data_product_status)
        if data_product_status == 'new':
            logger.info("Status is 'new', next step create documentation")
            return 'create_docs'
        elif data_product_status == 'to-be-validated':
            return 'waitting_manual_validation'
        elif data_product_status == 'to-be-publish':
            return 'validate_contract'
        else:
            return None #Or a different task for validated products, if needed.

with DAG(
    dag_id='data_product_validation_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'retries': 1,
    },
    schedule_interval=None,  # Run manually or trigger externally
    catchup=False,
    tags=['data_validation'],
) as dag:
    
    validate_status = PythonOperator(
        task_id='validate_status',
        python_callable=validate_data_product_status_and_process,
        provide_context=True,
    )


    create_docs = PythonOperator(
        task_id='create_docs',
        python_callable=create_documentation,
        provide_context=True,

    )

    attach_mandatory_contract_tags = PythonOperator(
        task_id='attach_mandatory_contract_tags',
        python_callable=attach_mandatory_contract_tags,
        provide_context=True,

    )

    create_contract_aspect_types = PythonOperator(
        task_id='create_contract_aspect_types',
        python_callable=create_contract_aspect_types,
        provide_context=True,
    )

    validate_contract = BranchPythonOperator(
        task_id='validate_contract',
        python_callable=validate_contract,
        provide_context=True,

    )

    share_data_product = PythonOperator(
        task_id='share_data_product',
        python_callable=share_data_product,
        provide_context=True,

    )
    
    mark_to_review = PythonOperator(
        task_id='mark_to_review',
        python_callable=mark_to_review,
        provide_context=True,

    )

    waitting_manual_validation = PythonOperator(
        task_id='waitting_manual_validation',
        python_callable=waitting_manual_validation,
        provide_context=True,

    )

    invalid_product = PythonOperator(
        task_id='invalid_product',
        python_callable=invalid_product,
        provide_context=True,

    )

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=branch_tasks,
        provide_context=True,
    )
    
    validate_status >> branching
    branching >> [create_docs,validate_contract,waitting_manual_validation]
    create_docs >> create_contract_aspect_types >> attach_mandatory_contract_tags >> mark_to_review
    validate_contract >> [invalid_product,share_data_product]
    