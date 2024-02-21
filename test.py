from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@flow(name='test',
      log_prints=True)
def lookup_to_bucket():

    path = './flows/london_postcodes.csv'

    gcs_block = GcsBucket.load("london-properties")
    gcs_block.upload_from_path(
        from_path=path,
        to_path='london_postcodes.csv'
    )

if __name__ == '__main__':
    lookup_to_bucket()