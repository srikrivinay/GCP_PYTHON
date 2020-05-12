from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime as datetime
from flask import Flask
import env_config as cfg
app = Flask(__name__)

credentials_data = service_account.Credentials.from_service_account_file('mtech-daas-product-pdata1-fb1c32d7fbb6.json')


def process_tablelist(search_key):
    table_list = []
    for project_name in cfg.bq_Projects:
        bq_client = bigquery.Client(credentials=credentials_data,project=project_name)
        query_for_schema = 'SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA'
        
        try:
            schemalist = bq_client.query(query_for_schema)
        except Exception as e:
            print('Error in {0} :: {1}'.format(project_name,e))
            
        for schema in schemalist:
            query_for_table = """SELECT table_catalog,table_schema,table_name,table_type 
            FROM """ + schema.schema_name + """.INFORMATION_SCHEMA.TABLES
            where LOWER(table_name) like '%""" + search_key.lower() + """%'"""
            
            try:
                tablelist = bq_client.query(query_for_table)
            except Exception as e:
                print('Error in {0} :: {1}'.format(project_name,e))
            
            if (len(list(tablelist.result())) > 0):
                for tablerow in tablelist:
                    table_list.append((tablerow[0],tablerow[1],tablerow[2],tablerow[3]))
    return table_list


def process_scriptlist(table_list):
    script_list = []
    storage_client = storage.Client(credentials=credentials_data,project=cfg.st_Project)
    bucket = storage_client.bucket(cfg.bucket_name)
    blobs = bucket.list_blobs()
    paths_list = list(blobs)
    for blob in paths_list:
        if (blob.name.endswith(cfg.sqlpattern)):
            filecontent = blob.download_as_string()
            for (prj,dset,table,tabltyp) in table_list:
                srch_string = cfg.bq_Projects[prj] + '}}`.' + dset + '.' + table
                if (eval("b'" + srch_string + "'") in filecontent):
                    script_list.append((prj,dset,table,tabltyp,blob.name))
    return script_list


def prepare_for_pythonlist(scriptlist):
    unique_prefix_list =[]
    prefix_dict = {}
    for scriptrow in scriptlist:
        script = scriptrow[4]
        folders = script.split('/')
        Prefix = folders[0]
        for i in range(1,len(folders)-2):
            Prefix = Prefix + '/' + folders[i]
        srch_string = folders[-2] + '/' + folders[-1]
        if (Prefix not in unique_prefix_list):
            unique_prefix_list.append(Prefix)
            prefix_dict[Prefix] = []
        if (script,srch_string) not in prefix_dict[Prefix]:
            prefix_dict[Prefix].append((script,srch_string))
    return (unique_prefix_list,prefix_dict)


def process_pythonlist(unique_prefix_list,prefix_dict):
    storage_client = storage.Client(credentials=credentials_data,project=cfg.st_Project)
    bucket = storage_client.bucket(cfg.bucket_name)
    python_list = {}
    for Prefix in unique_prefix_list:
        blobs = bucket.list_blobs(prefix=Prefix)
        paths_list = list(blobs)
        for blob in paths_list:
            if (blob.name.endswith(cfg.pypattern)):
                filecontent = blob.download_as_string()
                for (script,srch_string) in prefix_dict[Prefix]:
                    if script not in python_list:
                            python_list[script] = []
                    if (eval("b'" + srch_string + "'") in filecontent):
                        python_list[script].append(blob.name)
    return python_list


def insert_bq(data):
    bq_client = bigquery.Client(credentials=credentials_data,project=cfg.fnl_prj)
    Query = 'INSERT ' + cfg.fnl_table + ' VALUES ' + data
    insert_res = bq_client.query(Query)
    print(insert_res.result())

@app.route('/')
def main():
    search_key = input('Enter search keyword: ')
    print('Started for {0} at {1}'.format(search_key,datetime.now()))
    result_list = []
    print('Table list started at {}'.format(datetime.now()))
    table_list = process_tablelist(search_key)
    print('Table list ended at {}'.format(datetime.now()))
    if(len(table_list)) > 0:
        print('SQL list started at {}'.format(datetime.now()))
        script_list = process_scriptlist(table_list)
        print('SQL list ended at {}'.format(datetime.now()))
        if(len(script_list) > 0):
#            unique_Script_list = []
#            for i in range(len(script_list)):
#                if (script_list[i][4] not in unique_Script_list):
#                    unique_Script_list.append(script_list[i][4])
            (unique_prefix_list,prefix_dict) = prepare_for_pythonlist(script_list)
#            python_list = process_pythonlist(unique_Script_list)
            print('Python script list started at {}'.format(datetime.now()))
            python_list = process_pythonlist(unique_prefix_list,prefix_dict)
            print('Python script list ended at {}'.format(datetime.now()))
            for (prj,dset,table,tabltyp,script) in script_list:
                ts = str(datetime.now())
                if len(python_list[script]) > 0:
                    for i in range(len(python_list[script])):
                        python = cfg.pathextension + python_list[script][i]
                        result_list.append((prj,dset,table,tabltyp,cfg.pathextension + script,python,ts,ts,ts))
                else:
                    python = 'No python scripts found for the SQL'
                    result_list.append((prj,dset,table,tabltyp,cfg.pathextension + script,python,ts,ts,ts))
                    
                
        else:
            ts = str(datetime.now())
            for (prj,dset,table,tabltyp) in table_list:
                result_list.append((prj,dset,table,tabltyp,
                               'No SQLs found for the table','NA',ts,ts,ts))
        insert_bq(str(result_list)[1:-1]) # [1:-1] is to remove external square brackets[] of the list
    print('Ended for {0} at {1}'.format(search_key,datetime.now()))

if __name__ == '__main__':
    app.run()