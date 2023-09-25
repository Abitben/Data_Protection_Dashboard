def upload_to_bq(**kwargs):
    from include.sourcing.classes.upload_to_bq import UploadToBq
    import os
    from google.oauth2 import service_account
    from google.cloud import bigquery
    
    print("this is kwargs input:", kwargs)
    try:
        print(kwargs['xcom_value'][0])
        all_tables = kwargs['xcom_value'][0]
        for index, table_info in enumerate(all_tables):
            dataset = table_info["dataset"]
            table = table_info["table"]
            print('dataset', dataset)
            print('table', table)

            print('current path', os.getcwd())
            os.chdir('include')
            os.chdir('sourcing')
            print('changed path to this path', os.getcwd())
            credentials = service_account.Credentials.from_service_account_file('/usr/local/airflow/include/gcp/cnil-392113-c62bf34df38e.json')
            project_id = 'cnil-392113'
            client = bigquery.Client(credentials= credentials, project=project_id)
            print('logged in')
            UploadToBq(dataset, table)
    except IndexError:
        print("Aucune mise à jour n'a été détectée.")

    print("Chargement vers BigQuery terminé avec succès.")



