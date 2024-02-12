from prefect import flow, task 
from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import GcpCredentials, BigQueryWarehouse

# @flow
# def bigquery_external_table_create():