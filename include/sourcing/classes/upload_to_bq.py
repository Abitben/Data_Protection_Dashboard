import os
import pandas as pd
import time
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq

class UploadToBq:
    
    def __init__(self, dataset, table):
        print('---------------------UPLOAD TO BQ---------------------')
        self.project_id = 'cnil-392113'
        credentials = service_account.Credentials.from_service_account_file('/usr/local/airflow/include/gcp/cnil-392113-c62bf34df38e.json')
        self.credentials = credentials
        client = bigquery.Client(credentials= self.credentials, project= self.project_id)
        self.client = client
        self.dataset = dataset
        self.table = table
        self.df = pd.read_csv(f'datasets/{dataset}/{table}', sep=';')
        self.create_dataset()
        self.upload()
        print('---------------------NEXT---------------------')

    def create_dataset(self):
        dataset_id = f"cnil-392113.{self.dataset}".format(self.client.project)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        dataset = self.client.create_dataset(dataset, timeout=30, exists_ok=True) 
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))

    def upload(self):
        job_config = bigquery.LoadJobConfig(
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        field_delimiter=";"
        )

        table_bq = f'{self.dataset}.{self.table}'
        print(self.table, "---", self.df.shape)
        pandas_gbq.to_gbq(self.df, table_bq, project_id=self.project_id, if_exists='replace', credentials = self.credentials)
        print("Loaded {} rows into {}:{}.".format(self.df.shape[0], self.dataset, self.table))
