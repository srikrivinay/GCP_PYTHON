from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime as datetime
from flask import Flask, request
import env_config as cfg
import pytz
import threading

app = Flask(__name__)

credentials_data = service_account.Credentials.from_service_account_file('mtech-daas-product-pdata1-fb1c32d7fbb6.json')

def process_tablelist(search_key):
    global status,job_id
    table_list = []
    for project_name in cfg.bq_Projects:
        bq_client = bigquery.Client(credentials=credentials_data,project=project_name)
        #Get the datasets
        query_for_schema = 'SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA'
        schemalist = bq_client.query(query_for_schema)
        if (schemalist.errors != None):
            status = 'Error: table dataset listing'
            kill_process(status,job_id)
        for schema in schemalist:
            query_for_table = """SELECT table_catalog,table_schema,table_name,table_type 
            FROM """ + schema.schema_name + """.INFORMATION_SCHEMA.TABLES
            where LOWER(table_name) like '%""" + search_key.lower() + """%'"""
            #Get the resembling tables from datasets
            tablelist = bq_client.query(query_for_table)
            if (tablelist.errors != None):
                status = 'Error: table listing'
                kill_process(status,job_id)
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


def insert_bq(data,table):
    global status, job_id
    bq_client = bigquery.Client(credentials=credentials_data,project=cfg.fnl_prj)
    Query = 'INSERT ' + table + ' VALUES ' + data
    table_insert = bq_client.query(Query)
    if (table_insert.errors != None):
        status = 'Error: Inserting into ' + table
        kill_process(status,job_id)


def update_bq(status,job_id):
    global update_error
    update_error = False
    table = cfg.status_table
    bq_client = bigquery.Client(credentials=credentials_data,project=cfg.fnl_prj)
    Query = 'UPDATE ' + table + ' SET status = "' + status + '" WHERE job_id = "' + job_id + '"'
    table_update = bq_client.query(Query)
    if (table_update.errors != None):
        status = 'Error: Updating status table'
        update_error = True


def kill_process(status,job_id):
    update_bq(status,job_id)
    exit()


@app.route('/')
def main():
    global status, job_id
    status = ''
    args = request.args
    if ('key' in args) and ('jid' in args):
        search_key = args['key']
        job_id = args['jid']
        if ('lvl' in args):
            try:
                search_lvl = int(args['lvl'])
            except:
                search_lvl = 3
        else:
            search_lvl = 3
    else:
        #data = (key,jid,lvl,status)
        data = ('NA','Jxxxxxx',9,'Error in searchkey or job id at ' + str(datetime.now(pytz.timezone('US/Eastern'))))
        insert_bq(str(data),cfg.status_table)
        return 'You are on the wrong page'

    t1 = threading.Thread(target=process_main, args=(search_key,search_lvl,job_id,))
    t1.start()

    status = 'Started: Python process'
    update_bq(status,job_id)
    if (update_error):
        data = (search_key,job_id,search_lvl,'Error: updating status')
        insert_bq(str(data),cfg.status_table)
        exit()
    return 'Process Running: "' + job_id + '"'


def process_main(search_key,search_lvl,job_id):
    global status
    result_list = []
    table_list = process_tablelist(search_key)
    if(len(table_list) > 0 and search_lvl > 1):
        script_list = process_scriptlist(table_list)
        if(len(script_list) > 0 and search_lvl > 2):
            (unique_prefix_list,prefix_dict) = prepare_for_pythonlist(script_list)
            python_list = process_pythonlist(unique_prefix_list,prefix_dict)
            ts = str(datetime.now(pytz.timezone('US/Eastern')))
            for (prj,dset,table,tabltyp,script) in script_list:
                if len(python_list[script]) > 0:
                    for i in range(len(python_list[script])):
                        python = cfg.pathextension + python_list[script][i]
                        result_list.append((prj,dset,table,tabltyp,cfg.pathextension + script,python,search_lvl,ts))
                else:
                    python = 'No python scripts found for the SQL'
                    result_list.append((prj,dset,table,tabltyp,cfg.pathextension + script,python,search_lvl,ts))
        elif (len(script_list) > 0): 
            ts = str(datetime.now(pytz.timezone('US/Eastern'))) 
            for (prj,dset,table,tabltyp,script) in script_list:
                result_list.append((prj,dset,table,tabltyp,cfg.pathextension + script,'NA',search_lvl,ts))
        else:
            ts = str(datetime.now(pytz.timezone('US/Eastern')))
            for (prj,dset,table,tabltyp) in table_list:
                result_list.append((prj,dset,table,tabltyp,
                               'No SQLs found for the table','NA',search_lvl,ts))
    elif len(table_list) > 0:
        ts = str(datetime.now(pytz.timezone('US/Eastern')))
        for (prj,dset,table,tabltyp) in table_list:
            result_list.append((prj,dset,table,tabltyp,'NA','NA',search_lvl,ts))
    if (len(result_list) > 0):
        insert_bq(str(result_list)[1:-1],cfg.fnl_table) # [1:-1] is to remove external square brackets[] of the list
        status = 'Complete: Pyhton process completed'
    else:
        status = 'Complete: No table found'

    update_bq(status,job_id)
    if (update_error):
        data = (search_key,job_id,search_lvl,'Error updating')
        insert_bq(str(data),cfg.status_table)
        exit()

if __name__ == '__main__':
    app.run()