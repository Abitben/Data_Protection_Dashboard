import pandas as pd
import os
import re
from unidecode import unidecode


class CleanCSVForBq:

    def __init__(self, dataset, table):
        self.dataset = dataset
        self.table = table
        self.df = pd.read_excel(f'datasets/{dataset}/{table}', engine='openpyxl')
        print(f'datasets/{dataset}/{table}')
        self.transpose_df()
        print("Etat des colonnes avant traitement: ", self.df.columns)
        self.change_column()
        self.column_clean()
        print('check pour preparation des colonnes')
        self.check_column_clean()
        print("Etat des colonnes après traitement: ", self.df.columns)
        self.rename_duplicate_columns()
        self.return_csv()
        print('CSV was saved here :', f'datasets/{dataset}/{table}')
        print('---------------------NEXT---------------------')

    def transpose_df(self):
        l_need = ['open_cnil_ventilation_sanctions_depuis_2014_vd', 'opencnil_sanctions_depuis_2019_maj_aout_2022', "opencnil_dai_stic_judex_taj_maj_janvier_2019"]

        if self.df.shape[1] > self.df.shape[0] or self.table in l_need:
            print('Need to transpose')
            self.df = self.df.transpose()
            self.df.columns = self.df.iloc[0]
            self.df = self.df[1:]
            self.df = self.df.reset_index(names=[self.df.columns.name])
            self.df.columns.name = ""
            return self.df
        

    def change_column(self):
        try:
            if self.df.columns[1] == 'Unnamed: 1':
                self.df.columns = self.df.iloc[0]
                self.df = self.df[1:]
                self.df.columns.name = None
                return self.df
            elif self.df.columns[0] == 'Unnamed: 0' and self.df.columns[1] == 0:
                self.df.columns = self.df.iloc[0]
                self.df = self.df[1:]
                self.df.columns.name = None
                return self.df
            elif self.df.columns[2] == 'Unnamed: 1':
                self.df.columns = self.df.iloc[0]
                self.df = self.df[1:]
                self.df.columns.name = None
                return self.df
            else:
                print('No need to change column to first row')
        except IndexError:
            print('Le dataframe doit avoir au moins deux colonnes pour effectuer le changement.')
            self.df.columns = self.df.iloc[0]
            self.df = self.df[1:]
            self.df.columns.name = None
            return self.df

    def column_clean(self):
        new_columns = []
        for index, column in enumerate(self.df.columns):
            try:
                column = column.strip().lower().replace(" ", "_")
                column = unidecode(column)
                column = column.replace("\n", "_")
                column = column.replace(",", "")
                column = column.replace(".", "")
                column = column.replace("/", "")
                column = column.replace("(", "").replace(")","")
                column = column.replace("'", "")
                column = column.replace("-", "")
                column = column.replace("&", 'et')
                column = column.replace('https:edpbeuropaeuaboutedpbboardmembers_fr',"").replace('https:wwwafapdporglafapdpmembres',"")
                column = column.replace('<',"").replace('>', "")
                column = column.replace('[',"").replace(']', "")
                column = column[:200]
                new_columns.append(column)
            except AttributeError:
                new_columns.append(column)
                continue
    
        self.df.columns = new_columns
        return self.df

    import re

    def check_column_clean(self):
        for index, column in enumerate(self.df.columns):
            try: 
                pattern = r"^[a-zA-Z0-9_]+$"
                is_cleaned = re.match(pattern, column)

                if is_cleaned and len(column) <= 200:
                    print("La fonction column_clean a correctement nettoyé cette colonne.")
                else:
                    print("La fonction column_clean n'a pas correctement nettoyé la colonne. Re-exécution en cours...")
                    # Re-run the column_clean function
                    self.column_clean()
                    break 
            except TypeError:
                continue
        print("Re-exécution terminée.")
        return self.df


    def rename_duplicate_columns(self):
        column_count = {}
        new_columns = []

        for column in self.df.columns:
            if column in column_count:
                column_count[column] += 1
                new_column = f"{column}{column_count[column]}"
            else:
                column_count[column] = 1
                new_column = column

            new_columns.append(new_column)
        self.df.columns = new_columns
        return self.df

    def return_csv(self):
        self.df.to_csv(f'datasets/{self.dataset}/{self.table}', index=False, sep=";")