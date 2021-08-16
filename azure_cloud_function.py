# -*- coding: utf-8 -*-

from requests_api import DiligenceVaultHook
import pandas as pd
import json
import datetime
from azure.storage.blob import BlobClient
from azure.cosmosdb.table import TableService

AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=frontierappstorage;AccountKey=EeqJJzgDD4wt+TaD/jZxvxQ2t8KjkWD7fXN5D+hcNvgbYYnlngOho83WTfPMY4WxZe7uUgzWRO3kUzLmxFOR3A==;EndpointSuffix=core.windows.net"
container_dv = "diligencevault"
table_name_dv = "diligencevault"


def get_dataframe_from_table_storage_table(table_service, SOURCE_TABLE, filter_query):
    """ Create a dataframe from table storage data """
    return pd.DataFrame(get_data_from_table_storage_table(table_service, SOURCE_TABLE, filter_query))

def get_data_from_table_storage_table(table_service, SOURCE_TABLE, filter_query):
    """ Retrieve data from Table Storage """
    for record in table_service.query_entities(SOURCE_TABLE, filter=filter_query):
        yield record

def upload_blob_to_storage(file_name: str, file):
    blob_client = BlobClient.from_connection_string(
            # os.environ["AZURE_STORAGE_CONNECTION_STRING"],
            AZURE_STORAGE_CONNECTION_STRING,
            container_name=container_dv,
            blob_name=file_name
            )
    try:
        blob_client.upload_blob(data = file, overwrite=True)
        return True
    except Exception as e:
        print(f"{e}")
        return False
    

# initalize the DV Hook
dv_api = DiligenceVaultHook()
dv_api.get_token()

end_date=f'{datetime.datetime.now():%Y-%m-%d}'
start_date= f'{datetime.datetime.now()  - datetime.timedelta(days=365):%Y-%m-%d}'
status='Completed'

# download projections between a date
z = dv_api.download_projects(start_date=start_date, end_date=end_date, status=status)

for file in z.filelist:
    with z.open(file) as f:
        file_content = f.read()
        if file.filename == 'projects.json':
            new_project_data = json.loads(file_content)
           
            # get old projects.json from Azure check for changes and upload new 
            # combined file
            try:
                blob_client = BlobClient.from_connection_string(
                # os.environ["AZURE_STORAGE_CONNECTION_STRING"],
                AZURE_STORAGE_CONNECTION_STRING,
                container_name=container_dv,
                blob_name=file.filename
                )
                blob_data = blob_client.download_blob()
                old_project_data = json.loads(blob_data.content_as_bytes(max_concurrency=1))
            except Exception as e:
                # No blob so upload
                print(f"{e}")
                upload_blob_to_storage(file.filename, file_content)
                old_project_data = new_project_data  
            # project_id = id in the projects.json file this is the row_key
            # product_id = entity_id in the projects.json file this is the partition_key
            old_project_data_df = pd.DataFrame.from_dict(old_project_data)
            new_project_data_df = pd.DataFrame.from_dict(new_project_data)
            final_df = (pd.concat([old_project_data_df, new_project_data_df], ignore_index=True, sort=False)
                        .drop_duplicates(['id'], keep='last'))
            final_df["PartitionKey"] = final_df["entity_id"].astype(str)
            final_df["RowKey"] = final_df["id"].astype(str)
            final_df.drop(columns=['custom_fields'], inplace=True)
            upload_blob_to_storage(file.filename, final_df.to_json())
            # check table for new and submit there
            table_service = TableService(
                connection_string=AZURE_STORAGE_CONNECTION_STRING
                )
            
            projects_df = get_dataframe_from_table_storage_table(
                table_service=table_service,
                SOURCE_TABLE=table_name_dv,
                filter_query=""
                )
            
            if projects_df.empty:
                for row_dict in final_df.to_dict(orient="records"):
                    table_service.insert_or_replace_entity(table_name_dv, row_dict)
            else:
                combined_df = final_df.merge(projects_df, how='left', indicator=True)
                upload_df = combined_df[combined_df['_merge']=='left_only'].copy()
                upload_df.drop(columns=['_merge', 'Timestamp'], inplace=True, errors='ignore')
                for row_dict in upload_df.to_dict(orient="records"):
                    table_service.insert_or_replace_entity(table_name_dv, row_dict)            
        else:
            try:
                upload_blob_to_storage(file.filename, file_content)
            except Exception as e:
                raise f"{e}"
            

    
    
