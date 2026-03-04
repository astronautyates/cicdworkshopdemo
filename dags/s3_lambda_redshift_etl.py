"""
S3 → Lambda → Redshift ETL DAG

Generates dummy data across 5 parallel task instances, uploads CSVs to S3,
invokes a Lambda to consolidate them, then loads into Redshift.

AWS Setup Required:
    1. S3 bucket: njcourts-etl-data-v2
    2. Lambda function: njcourts-consolidate-csv (see include/lambda/consolidate.py)
    3. Redshift Serverless namespace/workgroup with table public.dummy_data
    4. IAM roles/policies (see include/setup/iam_policy.json)
    5. Airflow connection 'aws_default' configured with appropriate credentials

See include/setup/setup_instructions.md for full details.
"""

from __future__ import annotations

import csv
import io
import json
import random
import string
from datetime import datetime

from airflow.sdk import DAG, Asset, task

from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator

BUCKET = "njcourts-etl-data-v2"
S3_PREFIX = "incoming"
CONSOLIDATED_KEY = "consolidated/output.csv"
LAMBDA_FUNCTION = "njcourts-consolidate-csv"
REDSHIFT_TABLE = "public.dummy_data"
REDSHIFT_SCHEMA = "public"
REDSHIFT_WORKGROUP = "njcourts-wg"
REDSHIFT_DATABASE = "njcourts"
REDSHIFT_IAM_ROLE = "arn:aws:iam::944146428766:role/njcourts-redshift-s3-role"
NUM_PARTITIONS = 5
ROWS_PER_PARTITION = 100

CATEGORIES = ["Civil", "Criminal", "Family", "Traffic", "Probate"]

redshift_dummy_data = Asset("redshift://njcourts/public/dummy_data")


@task
def generate_dummy_data(partition: int) -> dict:
    """Generate a CSV string of dummy court data for a single partition."""
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["id", "name", "value", "category", "created_at"])

    for i in range(ROWS_PER_PARTITION):
        row_id = partition * ROWS_PER_PARTITION + i
        name = "".join(random.choices(string.ascii_letters, k=8))
        value = round(random.uniform(1.0, 9999.99), 2)
        category = random.choice(CATEGORIES)
        created_at = datetime.now().isoformat()
        writer.writerow([row_id, name, value, category, created_at])

    return {"partition": partition, "csv_data": buf.getvalue()}


@task
def upload_to_s3(record: dict) -> None:
    """Upload a single partition CSV to S3."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id="aws_default")
    key = f"{S3_PREFIX}/part_{record['partition']}.csv"
    hook.load_string(
        string_data=record["csv_data"],
        bucket_name=BUCKET,
        key=key,
        replace=True,
    )


with DAG(
    dag_id="s3_lambda_redshift_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "s3", "lambda", "redshift"],
    doc_md=__doc__,
) as dag:

    partitions = list(range(NUM_PARTITIONS))

    csv_data = generate_dummy_data.expand(partition=partitions)

    uploaded = upload_to_s3.expand(record=csv_data)

    check_s3_files = S3KeySensor(
        task_id="check_s3_files",
        bucket_name=BUCKET,
        bucket_key=[f"{S3_PREFIX}/part_{i}.csv" for i in partitions],
        aws_conn_id="aws_default",
        poke_interval=10,
        timeout=300,
    )

    invoke_lambda = LambdaInvokeFunctionOperator(
        task_id="invoke_lambda",
        function_name=LAMBDA_FUNCTION,
        payload=json.dumps(
            {
                "bucket": BUCKET,
                "source_prefix": S3_PREFIX,
                "output_key": CONSOLIDATED_KEY,
                "num_files": NUM_PARTITIONS,
            }
        ),
        aws_conn_id="aws_default",
        region_name="us-east-1",
    )

    load_to_redshift = RedshiftDataOperator(
        task_id="load_to_redshift",
        database=REDSHIFT_DATABASE,
        workgroup_name=REDSHIFT_WORKGROUP,
        sql=f"""
            COPY {REDSHIFT_TABLE}
            FROM 's3://{BUCKET}/{CONSOLIDATED_KEY}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            CSV
            IGNOREHEADER 1
            TIMEFORMAT 'auto';
        """,
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        poll_interval=5,
        outlets=[redshift_dummy_data],
    )

    uploaded >> check_s3_files >> invoke_lambda >> load_to_redshift
