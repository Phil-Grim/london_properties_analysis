from prefect import flow
# from flows import web_to_gcs_bq

if __name__ == '__main__':
    flow.from_source(
        source="https://github.com/Phil-Grim/london_properties_analysis.git",
        entrypoint="flows/web_to_gcs_bq.py:main_flow"
    ).deploy(
        name="rightmove_flow",
        work_pool_name="rightmove-managed-pool",
        cron="12 20 * * *"
    )

