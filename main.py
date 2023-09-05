import functions_framework
from google.cloud import bigquery
from datetime import datetime, timedelta

@functions_framework.http
def backup_dataset(request):
    client = bigquery.Client()

    # the source porject ID and the dataset containing the tables
    source_project = 'source_product_id'
    source_dataset = 'source_dataset_id'
    
    #target project ID where backups will be stored. Ensure that project has necessory access
    target_project = 'target_project_id' 
    date_str = datetime.now().strftime('%Y_%m_%d')  # create a date string
    target_dataset_name = f'backup_{date_str}' #create backup dataset name

    # Check if the target dataset already exists
    target_dataset_ref = bigquery.DatasetReference(target_project, target_dataset_name)
    try:
        target_dataset = client.get_dataset(target_dataset_ref)
        print(f"Using existing dataset: {target_dataset_name}")
    except Exception as e:
        # Create a new dataset with the current date if not found
        target_dataset = bigquery.Dataset(target_dataset_ref)
        target_dataset.location = 'US'  # Set the appropriate location
        target_dataset = client.create_dataset(target_dataset)
        print(f"Created new dataset: {target_dataset_name}")

    # Copy tables from the source dataset to the target dataset
    tables = client.list_tables(f'{source_project}.{source_dataset}')
    for table in tables:
        source_table = f'{source_project}.{source_dataset}.{table.table_id}'
        target_table = f'{target_project}.{target_dataset_name}.{table.table_id}'
        copy_job = client.copy_table(source_table, target_table)
        copy_job.result()

    # Delete backups that are older than 14 days
    old_date_str = (datetime.now() - timedelta(days=14)).strftime('%Y_%m_%d')
    old_dataset_name = f'expletus_media_backup_{old_date_str}'
    old_dataset_ref = bigquery.DatasetReference(target_project, old_dataset_name)

    try:
        old_dataset = client.get_dataset(old_dataset_ref)  # Check if the old dataset exists
        client.delete_dataset(old_dataset_ref, delete_contents=True)
        print(f"Deleted old dataset: {old_dataset_name}")
    except Exception as e:
        print(f"Old dataset {old_dataset_name} does not exist. No deletion required.")
    
    return 'Backup operation completed successfully!'