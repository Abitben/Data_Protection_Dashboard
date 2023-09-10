import requests
import pandas as pd
from unidecode import unidecode
import os
import urllib3



class GetCnilIndexes:
    def __init__(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.api_url = 'https://www.data.gouv.fr/api/1/organizations/534fff61a3a7292c64a77d59/catalog'
        self.headers = {'accept': 'application/json'}
        self.tables = self.fetch_data_from_api()


        if self.tables:
            self.df_tables = self.process_tables_data()
            self.df_datasets = self.load_datasets_info()
            if self.df_datasets is not None:
                self.clean_datasets_info()
            else:
                print('Le chargement des informations sur les datasets a échoué.')
            self.df_tables = self.merge_info_and_clean()
            self.df_indexes = self.clean_columns()
            self.save_to_csv()

        else:
            print('La requête a échoué ou aucune donnée n\'a été trouvée.')

    def fetch_data_from_api(self):
        response = requests.get(self.api_url, headers=self.headers)
        if response.status_code == 200:
            print('La requête est un succès:', response.status_code)
            data = response.json()
            return data.get('@graph', [])
        else:
            print('La requête a échoué avec le code d\'erreur :', response.status_code)
            return []

    def process_tables_data(self):
        processed_data = []
        for table in self.tables:
            data = {
                'id': table.get('identifier'),
                'accessURL': table.get('@id'),
                'title': table.get('title'),
                'format': table.get('format'),
                'last_updated': table.get('modified'),
                'download_url': table.get('downloadURL')
            }
            processed_data.append(data)
        return pd.DataFrame(processed_data).dropna()

    def load_datasets_info(self):
        dataset_url = 'https://www.data.gouv.fr/fr/organizations/cnil/datasets.csv'
        try:
            return pd.read_csv(dataset_url, sep=';')
        except Exception as e:
            print('Erreur lors du chargement des informations sur les datasets :', e)
            return None

    def clean_datasets_info(self):
        def find_dataset_id(row):
            for dataset_id in self.df_datasets.id:
                if dataset_id in row['accessURL']:
                    return dataset_id
            return None

        self.df_tables['dataset_id'] = self.df_tables.apply(find_dataset_id, axis=1)

    def merge_info_and_clean(self):
        self.df_indexes = self.df_tables.merge(self.df_datasets[['id', 'slug', 'frequency']], left_on = 'dataset_id', right_on='id')
        self.df_indexes = self.df_indexes.drop(columns=['id_y'])
        self.df_indexes = self.df_indexes.rename(columns={"id_x" : "table_id"})
        self.df_indexes = self.df_indexes[self.df_indexes.format == 'xlsx'] 
        self.df_indexes = self.df_indexes.reset_index()
        self.df_indexes = self.df_indexes.drop(columns=['index'])
        return self.df_indexes
        
    def clean_columns(self):

        def format_title(string):
            return unidecode(string.replace(".xlsx", "").lower().replace('-', "_").replace(' ', "_"))
            
        self.df_indexes['last_updated'] = pd.to_datetime(self.df_indexes['last_updated'])
        self.df_indexes['title'] = self.df_indexes['title'].apply(lambda row: format_title(row))
        self.df_indexes['slug'] =  self.df_indexes['slug'].apply(lambda row: row.replace('-', "_"))
        return self.df_indexes

    def save_to_csv(self):
        os.makedirs('metadata', exist_ok=True)
        self.df_indexes.to_csv('metadata/cnil_dataset_metadata.csv')
        print("Le fichier de CNIL index a bien été enregistré ici", './metadata/cnil_dataset_metadata.csv')
        print("Pour accéder au Dataframe : variable.df_all")
        return self.df_indexes
