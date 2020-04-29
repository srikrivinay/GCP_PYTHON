from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import NotFound
# Explicitly use service account credentials by specifying the private key
# file.

credentials_data = service_account.Credentials.from_service_account_file('C:/Users/nagasudhindra.g/Desktop/python/json/mtech-daas-testdata-cbc529459fa8.json')
project_id = ['mtech-daas-testdata','mtech-daas-product-pdata1', 'mtech-daas-product-sdata1']

for project_name in project_id:
    client = bigquery.Client(credentials=credentials_data, project=project_name)
    query_table1 = 'SELECT schema_name  FROM  INFORMATION_SCHEMA.SCHEMATA;'

    try:
        query_job1 = client.query(query_table1)
    except Exception as e:
        print("Error in project_name")
        print(e)

    for dataset_name in query_job1:
        #print(dataset_name[0])
        query_table2 = 'SELECT * FROM `' + dataset_name[0] + '.INFORMATION_SCHEMA.TABLES`;'
        query_job2 = client.query(query_table2)
        #print(type(query_job2))
        for row in query_job2:
             print(row[0:-1])
             

        '''if (len(list(query_job2.result())) > 0):
            print('{2:30} | {0:30} | {1:30}'.format('Schema', 'Table', 'Project'))
            for table_row in query_job2:
                print('{2:30} | {0:30} | {1:30}'.format(dataset_name.schema_name, table_row.table_name, project_id))'''

 #   try:
 #       query_job1 = client.query(query_table)
 #       print("The query data:")
 #       for row_output in query_job1:
 #           print(row_output)
 #  except Exception as e:
 #       print("Error in dataset_name")
 #       print(e)
