# -*- coding: utf-8 -*-

from requests_api import DiligenceVaultHook
import pandas as pd
import json

# initalize the DV Hook
dv_api = DiligenceVaultHook()
dv_api.get_token()

# get project list
project_list = dv_api.get_projects()

# download projections between a date
z = dv_api.download_projects(start_date='2020-06-30', end_date='2020-12-31')
z.extractall('./downloads')

# we could work in memomry and not exctract all
df_responses = pd.DataFrame()
for file in z.filelist:    
    if file.filename.endswith('responses.json'):
        with z.open(file) as f:  
            data = f.read()  
            d = json.loads(data)
            df_nested_response = pd.DataFrame.from_dict(d)
            # df_flat_repsonse = (pd.concat({i: pd.DataFrame(x) for i, x in df_nested_response.pop('response').items()}).reset_index(level=1, drop=True).join(df_nested_response).reset_index(drop=True))
            # df_super_flat_response = pd.json_normalize(d, ['response'], errors='ignore')
            df_responses = pd.concat([df_responses, df_nested_response],ignore_index=True)
    elif file.filename.endswith('projects.json'):
        with z.open(file) as f:  
            data = f.read()  
            d = json.loads(data)
            df_main = pd.DataFrame.from_dict(d)
        
df_responses['project_id'] = df_responses['project_id'].astype(int)
df_main['id'] = df_main['id'].astype(int)
                
df_all = pd.merge(
    df_main,
    df_responses,
    how="inner",
    left_on="id",
    right_on="project_id"
)


#  read in DV json response file, flatten nested table reponses into dictionary with dataframes

file_path = 'C:/OneDrive/Frontier Advisors Pty Ltd/Frontier Advisors - OneDrive/GSampson Briefcase/GitHub/DV_API/downloads/23328_AlphaFixedIncome/responses/responses.json'

with open(file_path) as f:  
    data = f.read()
    dv_data_dict = dv_api.json_response_data_to_dict(data)
    
