# -*- coding: utf-8 -*-

# http://docs.python-requests.org/en/master/api/
import requests
import json 
import pandas as pd
import zipfile, io
import re
from config import DiligenceVaultsConfig

class DiligenceVaultHook():
    folder_path = 'C:/OneDrive/Frontier Advisors Pty Ltd/Frontier Advisors - OneDrive/GSampson Briefcase/GitHub/DV_API/downloads/'
    projects_file_name = 'projects.json'
    response_paths = '/responses/responses.json'
    
    def __init__(self, config=None, **kwargs):
        self.session = requests.Session()
        self.config = ""
        self.api_url = ""
        self.api_key = ""
        self.token = ""
        self.initalize(config)
        self.headers = {'X-API-KEY': self.api_key, "Accept": "application/json"}
        # self.get_token() #just commenting out to allow running offline
        self.result_headings = ['project_id', 'entity_name', 
                  'section_id', 'section_name', 
                  'subsection_id', 'subsection_name',
                  'sequence_id', 'question_id', 'response_type',  'text', 'value']
        
        for arg in kwargs:
            if isinstance(kwargs[arg], dict):
                kwargs[arg] = self.__deep_merge(getattr(self.session, arg), kwargs[arg])
            setattr(self.session, arg, kwargs[arg])
    
    def initalize(self, config):
        if config == "PROD":
            self.config = DiligenceVaultsConfig.Production
            self.api_url= self.config['API_URL']
            self.api_key= self.config['API_KEY']
        else:
            self.config = DiligenceVaultsConfig.Test
            self.api_url= self.config['API_URL']
            self.api_key= self.config['API_KEY']
            
    def token_check(self):
        if self.token == "":
            self.get_token()
            
    def get_token(self):
        r = self.get("v1/get-token/", headers=self.headers)
        token = r.json()
        self.token = token['access_token']
        self.headers.update({"Authorization": f"Bearer {self.token}"})

    def get_firms(self):
        # List out all the firms and related information        
        r = self.get("v1/firms", headers=self.headers)
        return r.json()
    
    def get_products(self):
        # List out all the products and related information
        r = self.get("v1/products", headers=self.headers)
        return r.json()
    
    def get_projects(self, start_date=None, end_date=None, status=None, date_type=None):
        # start_date(optional) YYYY-MM-DD
        # end_date(optional) YYYY-MM-DD
        # status(optional) can single or multiple with comma separated value(started,completed,invited)
        # date_type = ‘started_at’,’completed_at’, ‘created_at’, ‘updated_at’
        #   created_at , completed_at, Started_at = For project’s started, completed and created date
        #   updated_at = Any responses which updated in given time span
        
        params = {}
        if start_date is not None:
            params.update({"start_date":start_date})
            
        if end_date is not None:
            params.update({"end_date":end_date})
            
        if status is not None:
            params.update({"status":status})
            
        if date_type is not None:
            params.update({"date_type":date_type})

        r = self.get("v1/projects", params=params, headers=self.headers)
        return r.json()
    
    def get_projects_by_template(self, start_date=None, end_date=None, status=None, date_type=None, template=None):
        # start_date(optional) YYYY-MM-DD
        # end_date(optional) YYYY-MM-DD
        # status(optional) can single or multiple with comma separated value(started,completed,invited)
        # date_type = ‘started_at’,’completed_at’, ‘created_at’, ‘updated_at’
        #   created_at , completed_at, Started_at = For project’s started, completed and created date
        #   updated_at = Any responses which updated in given time span
        
        params = {}
        if start_date is not None:
            params.update({"start_date":start_date})
            
        if end_date is not None:
            params.update({"end_date":end_date})
            
        if status is not None:
            params.update({"status":status})
            
        if date_type is not None:
            params.update({"date_type":date_type})

        r = self.get("v1/projects", params=params, headers=self.headers)
        
        df_projects = pd.DataFrame.from_dict(r.json())
        
        result = df_projects.loc[df_projects['template_name'].isin([template])]
        
        return result
       
    def download_projects(self, start_date, end_date, status=None, date_type=None, exclude_documents=True, exclude_notes=True, **kwargs):
           # start_date YYYY-MM-DD
           # end_date YYYY-MM-DD
           # status(optional) can single or multiple with comma separated value(started,completed,invited)
           # date_type = ‘started_at’,’completed_at’, ‘created_at’, ‘updated_at’
           #   created_at , completed_at, Started_at = For project’s started, completed and created date
           #   updated_at = Any responses which updated in given time span
           # Optional kawargs: exclude_documents=boolean, exclude_notes=boolean, status=""or["",""] , date_type=""
           # Possible values for the status(single or multiple with comma separated): Invited, Started, Followup, Completed, Approved, NotApproved, ExtensionRequested, PendingRestart
           # Possible values for the date_type: created_at, completed_at, started_at, updated_at
           #    --> created_at , completed_at, Started_at = For project’s started, completed and created date
           #    --> updated_at = Any responses which updated in given time span
        
        ### returns zipfile of jsons ###
           
        params = {}

        r = re.compile('\d{4}-\d{2}-\d{2}')
        if r.match(start_date) is None:
            raise ValueError("Incorrect start date format, should be YYYY-MM-DD")
        
        if r.match(end_date) is None:
            raise ValueError("Incorrect start date format, should be YYYY-MM-DD")

        for key, value in locals().items():
        # loop args passed and build params
            if key is not 'kwargs':
                if value is not None:
                    params.update({key:value})
        
        additional_params = dict([i for i in kwargs.items() if i[1] != None])

        if additional_params is not None:
            params.update(additional_params)
        
        r = self.get("v1/projects/projects_download", params=params, headers=self.headers)
        
        z = zipfile.ZipFile(io.BytesIO(r.content))
        
        return z
    
    def download_projects_by_id(self, project_ids=None):
        # project_ids string array [XXX,XXX]
        
        if project_ids is None: 
            projects = self.get_projects()
            df_projects = pd.DataFrame.from_dict(projects)
            _project_ids = {"projects": df_projects['id'].astype(str).values.tolist()}
        else:
            _project_ids = {"projects": project_ids}
        
        r = self.post("v1/projects/projects_download", json=_project_ids, headers=self.headers, stream=True)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        
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
        
        return df_all
           
    def request(self, method, url, **kwargs):
        return self.session.request(method, self.api_url+url, **kwargs)

    def head(self, url, **kwargs):
        return self.session.head(self.api_url+url, **kwargs)

    def get(self, url, **kwargs):
        return self.session.get(self.api_url+url, **kwargs)

    def post(self, url, **kwargs):
        return self.session.post(self.api_url+url, **kwargs)

    def put(self, url, **kwargs):
        return self.session.put(self.api_url+url, **kwargs)

    def patch(self, url, **kwargs):
        return self.session.patch(self.api_url+url, **kwargs)

    def delete(self, url, **kwargs):
        return self.session.delete(self.api_url+url, **kwargs)

    def response_type_attachment(self, response):
        #Attachment
        return True
        
    def response_type_aumtable(self, response):
        #AUM History
        return True
        
    def response_type_dynamicgrid(self, response):
        #Customize Grid / Table
        return self.response_type_grid(response)
        
    def response_type_date(self, response):
        #Date
        return response['value']
        
    def response_type_textemail(self, response):
        #Email
        return True
        
    def response_type_grid(self, response):
        #flatten out the response to a nice dataframe
        respose_data = response['table_data'].reset_index(drop=True)[0]
        dfT = None
        try:
            df = pd.DataFrame.from_dict(respose_data)
            df.drop(['column_options', 'column_types', 'rows'], axis=1, inplace=True, errors="ignore")
            df.set_index('columns', inplace=True)
            dfT = df.T
        except:
            try:
                df = pd.DataFrame.from_dict(respose_data, orient='index').T
                df.drop(['column_options', 'column_types', 'rows'], axis=1, inplace=True, errors="ignore")
                df.set_index('columns', inplace=True)
                dfT = df.T
            except:
                try:
                    df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in respose_data.items() ]))
                    df.set_index('columns', inplace=True)
                    df.drop(['column_options', 'column_types', 'rows'], axis=1, inplace=True, errors="ignore")
                    dfT = df.T
                except Exception as e:
                    raise ValueError(str(e))
                    
        dfT.drop(list(dfT.filter(regex = 'None')), axis = 1, inplace = True)
        return dfT
        
    def response_type_identifier(self, response):
        #Identifier
        return True
        
    def response_type_checkBox(self, response):
        #List of multiple choices / Checkbox
        return True
        
    def response_type_dropdown(self, response):
        #List of single choice / Dropdown
        return True
        
    def response_type_bookends(self, response):
        #Min-max range
        return True
        
    def response_type_numeric(self, response):
        #Numeric – Decimal
        return True
        
    def response_type_integer(self, response):
        #Numeric – Integer
        return True
        
    def response_type_percentage(self, response):
        #Percentage
        return True
        
    def response_type_textphone(self, response):
        #Phone(self, response):
        return True
        
    def response_type_text(self, response):
        #Text Explanation - One Line
        return True
        
    def response_type_textmultiline(self, response):
        #Text Explanation – Paragraph
        return True
        
    def response_type_returntable(self, response):
        #Track Record
        return True
        
    def response_type_boolean(self, response):
        #Yes/No
        return True
        
    def response_type_noplus(self, response):
        #Yes/No with explanation for No
        return True
        
    def response_type_booleanplus(self, response):
        #Yes/No with explanation for Yes
        return True
    
    def responses_df_to_dict(self, df_nested_response):
        # flatten any nested responses into dataframes
        # return a nice dictionary
        df_flat_repsonse = (pd.concat({i: pd.DataFrame(x) for i, x in df_nested_response.pop('response').items()}).reset_index(level=1, drop=True).join(df_nested_response).reset_index(drop=True))
        results = df_flat_repsonse[self.result_headings].to_dict(orient='records')
        
        for dic in results:
            if dic['response_type'] == 'type_grid':
                response = df_flat_repsonse.loc[df_flat_repsonse['question_id']== dic['question_id']]
                value = self.response_type_grid(response)
                dic['value'] = value
        return results
    
    def downloaded_json_file_to_df(self, file_path):
        with open(file_path) as f:  
            data = f.read()        
            f.close()
        d = json.loads(data)
        df = pd.DataFrame.from_dict(d)
        return df
    
    def load_json_file(self, file_path):
        with open(file_path) as f:  
            data = f.read()        
            f.close()        
        return data
    
    def get_radias_infra_responses(self, start_date, end_date):
        # flatten any nested responses into dataframes
        # return a nice dictionary
        df_flat_repsonse = (pd.concat({i: pd.DataFrame(x) for i, x in df_nested_response.pop('response').items()}).reset_index(level=1, drop=True).join(df_nested_response).reset_index(drop=True))
        results = df_flat_repsonse[self.result_headings].to_dict(orient='records')
        
        for dic in results:
            if dic['response_type'] == 'type_grid':
                response = df_flat_repsonse.loc[df_flat_repsonse['question_id']== dic['question_id']]
                value = self.response_type_grid(response)
                dic['value'] = value
        return results
    

# =============================================================================
#     def result(self, project_list):        
#         if project_ids is None: 
#             projects = self.get_projects()
#             df_projects = pd.DataFrame.from_dict(projects)
#             _project_ids = {"projects": df_projects['id'].astype(str).values.tolist()}
#         else:
#             _project_ids = {"projects": project_ids}
#         
#         r = self.post("v1/projects/projects_download", json=_project_ids, headers=self.headers, stream=True)
#         z = zipfile.ZipFile(io.BytesIO(r.content))
#         
#         df_responses = pd.DataFrame()
#         
#         for file in z.filelist:
#             if file.filename.endswith('responses.json'):
#                 with z.open(file) as f:  
#                     data = f.read()  
#                     d = json.loads(data)
#                     df_nested_response = pd.DataFrame.from_dict(d)
#                     # df_flat_repsonse = (pd.concat({i: pd.DataFrame(x) for i, x in df_nested_response.pop('response').items()}).reset_index(level=1, drop=True).join(df_nested_response).reset_index(drop=True))
#                     # df_super_flat_response = pd.json_normalize(d, ['response'], errors='ignore')
#                     df_responses = pd.concat([df_responses, df_nested_response],ignore_index=True)
#             elif file.filename.endswith('projects.json'):
#                 with z.open(file) as f:  
#                     data = f.read()  
#                     d = json.loads(data)
#                     df_main = pd.DataFrame.from_dict(d)
#                     
#         df_responses['project_id'] = df_responses['project_id'].astype(int)
#         df_main['id'] = df_main['id'].astype(int)
#                             
#         df_all = pd.merge(
#             df_main,
#             df_responses,
#             how="inner",
#             left_on="id",
#             right_on="project_id"
#         )
#         
#         return df_all
# =============================================================================


    @staticmethod
    def __deep_merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict):
                node = destination.setdefault(key, {})
                DiligenceVaultHook.__deep_merge(value, node)
            else:
                destination[key] = value
        return destination
