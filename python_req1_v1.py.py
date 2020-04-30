from google.cloud import bigquery
from google.oauth2 import service_account

credentials_data = service_account.Credentials.from_service_account_file('mtech-daas-product-pdata1-fb1c32d7fbb6.json')
project_id_list = ['mtech-daas-testdata','mtech-daas-product-pdata1', 'mtech-daas-product-sdata1']

search_key = input('Please enter search key: ')
search_result = []

for project_id in project_id_list:
    client = bigquery.Client(credentials=credentials_data,project=project_id)
    query_for_schema = """SELECT schema_name FROM INFORMATION_SCHEMA.SCHEMATA"""
    
    try:
        query_schemalist = client.query(query_for_schema)
    except Exception as e:
        print("Error in project_name {}".format(project_id))
        print(e)
        
    for schema_row in query_schemalist:
        query_for_table = """SELECT * FROM """ + schema_row.schema_name + """.INFORMATION_SCHEMA.TABLES
                       where LOWER(table_name) like '%""" + search_key.lower() + """%'"""

        try:
            query_tablelist = client.query(query_for_table)
        except Exception as e:
            print("Error in schema {0} of project {1}".format(schema_name,project_id))
            print(e)

        if (len(list(query_tablelist.result())) > 0):
            for table_row in query_tablelist:
                search_result.append((project_id,schema_row.schema_name,table_row.table_name,table_row.table_type))


if (len(search_result) > 0):
    print('\n\n{0:30} | {1:30} | {2:30} | {3:30}'.format('Project','Schema','Table','Component type'))
    print('{0} | {0} | {0} | {0}'.format('-' * 30))
    for result in search_result:
        print('{0:30} | {1:30} | {2:30} | {3:30}'.format(result[0],result[1],result[2],result[3]))
