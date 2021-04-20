# -*- coding: utf-8 -*-

from requests_api import DiligenceVaultHook
import pandas as pd
import json

# initalize the DV Hook
dv_api = DiligenceVaultHook()
self = dv_api

folder_path = 'C:/OneDrive/Frontier Advisors Pty Ltd/Frontier Advisors - OneDrive/GSampson Briefcase/GitHub/DV_API/downloads/'
file_name = 'projects.json'
response_paths = '/responses/responses.json'

def responses_df_to_dict(self, df_nested_response):
    # flatten any nested responses into dataframes
    # return a nice dictionary
    df_flat_repsonse = (pd.concat({i: pd.DataFrame(x) for i, x in df_nested_response.pop('response').items()}).reset_index(level=1, drop=True).join(df_nested_response).reset_index(drop=True))
    results = df_flat_repsonse[self.result_headings].to_dict(orient='records')
    
    for dic in results:
        if dic['response_type'] == 'type_grid' or dic['response_type'] == 'type_dynamicgrid':
            response = df_flat_repsonse.loc[df_flat_repsonse['question_id']== dic['question_id']]
            value = self.response_type_grid(response)
            dic['value'] = value
    return results

def generate_reponse_file_paths(project_list):
    project_list['file_path'] = project_list['entity_name'].str.replace(' ','')
    try:
        project_list['file_path'] = project_list['id'].astype(str) + "_" + project_list['file_path'].astype(str) + response_paths
    except:
        raise ValueError("missing either id or entity name from the project_list input dataframe")
        

project_list = dv_api.downloaded_json_file_to_df(folder_path + file_name)
project_list = project_list.loc[project_list['template_name'] == 'RADIAS - Infrastructure December']
project_list = project_list.loc[project_list['status'] == 'Completed']
generate_reponse_file_paths(project_list)

project_list['responses'] = project_list.apply(lambda x: dv_api.downloaded_json_file_to_df(folder_path + x['file_path']), axis=1)

responses = project_list['responses'].iloc[0]

response_trans = responses_df_to_dict(self, responses)

# =============================================================================
# For RADIAS Infra 
# first step is to load all the funds and assets
# then load fund holdings
# then load attributes master data
# then load data
# =============================================================================





