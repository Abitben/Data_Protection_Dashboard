from google.cloud import bigquery
import pandas as pd
import datetime
from datetime import datetime
from google.oauth2 import service_account
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from xvfbwrapper import Xvfb
import time
import os


class CheckUpdateCnil:

    def __init__(self) -> None:
        self.credentials = service_account.Credentials.from_service_account_file('/usr/local/airflow/include/gcp/cnil-392113-c62bf34df38e.json')
        self.df_index = pd.read_csv('./metadata/cnil_dataset_metadata.csv', index_col=0)
        self.df_index['last_updated'] = pd.to_datetime(
            self.df_index['last_updated'])
        self.df_index['title'] = self.df_index['title'].str.replace(r'_\d{8}', '', regex=True)
        self.get_bq_modified_date()
        self.check_if_updated()


    def get_bq_modified_date(self):
        print('Getting BigQuery modified dates...')

        client = bigquery.Client.from_service_account_json(
            "/usr/local/airflow/include/gcp/cnil-392113-c62bf34df38e.json")
        dataset_list = list(client.list_datasets())
        dataset_ids = []
        table_ids = []
        modified_dates = []
        for dataset_item in dataset_list:
            dataset = client.get_dataset(dataset_item.reference)
            tables_list = list(client.list_tables(dataset))

            for table_item in tables_list:
                table = client.get_table(table_item.reference)
                dataset_ids.append(dataset.dataset_id)
                table_ids.append(table.table_id.lower())
                modified_dates.append(table.modified)
        data = {
            'bq_dataset': dataset_ids,
            'bq_table': table_ids,
            'bq_modified': modified_dates
        }
        print('Done.')
        self.df_bq = pd.DataFrame(data)
        self.df_bq['bq_modified'] = self.df_bq['bq_modified'].dt.tz_localize(None)
        self.df_bq['bq_table'] = self.df_bq['bq_table'].str.replace(r'_\d{8}', '', regex=True)

    def check_if_updated(self):
        df = self.df_index.merge(self.df_bq, how='left', left_on=['slug', 'title'], right_on=['bq_dataset', 'bq_table'])
        df['updated'] = df.apply(lambda row: row['bq_modified'] < row['last_updated'], axis=1)
        df['updated'] = df['bq_modified'].isnull()
        if df['updated'].any() or df['bq_table'].isnull().any():
            print('Some tables are not updated : self.df_updated to get info')
            self.df_updated = df[(df['updated'] == True) | (df['bq_table'].isnull())]
            return self.df_updated
        else:
            print('All tables are updated : self.df_updated to get info')
            self.df_updated = df

    def check_new_sanctions_fr(self):
        print('Checking new sanctions...')
        url = 'https://www.cnil.fr/fr/les-sanctions-prononcees-par-la-cnil'
        dfs_sanctions = pd.read_html(url)
        df_sanctions_online_last = dfs_sanctions[0]
        year = datetime.now().year
        project_id = 'cnil-392113'
        dataset_id = 'sanctions_prononcees_par_la_cnil'
        table = f'listes_sanctions_{year}'
        query_string = f'SELECT * FROM `{project_id}.{dataset_id}.{table}`;'
        df_sanctions = pd.read_gbq(query_string, credentials=self.credentials)
        df_sanctions['date'] = pd.to_datetime(df_sanctions['date'])
        df_sanctions_online_last['Date'] = pd.to_datetime(df_sanctions_online_last['Date'])
        if df_sanctions_online_last['Date'].max() > df_sanctions['date'].max():
            print('last sanction :', df_sanctions_online_last['Date'].max())
            print('New sanctions found for france : self.df_sanctions to get info', f'new csv file created here : datasets/sanctions_prononcees_par_la_cnil/listes_sanctions_{year}')
            df_sanctions_online_last.to_excel(f'datasets/sanctions_prononcees_par_la_cnil/listes_sanctions_{year}', engine='openpyxl')
            dataset = 'sanctions_prononcees_par_la_cnil'
            table = f'listes_sanctions_{year}'
            self.dataset_sanc_fr = dataset
            self.table_sanc_fr = table
            return True
        else:
            print('No new sanctions found for france.')
            return False
        print('Done.')
    
    def check_new_decisions_eu(self):
        self.get_new_sanctions_eu()
        if self.df_sanc_eu_updt.shape[0] > 0:
            print('New sanctions found : self.df_sanctions_eu_updt to get info, beginning to update csv file')
            self.concat_new_sanctions_eu()
            print(self.df_sanc_eu_updt.sort_values(by=['etid_number'], ascending=False).head(5))
            self.save_sanc_eu_csv()
            return True

    def get_new_sanctions_eu(self):
        self.get_sanctions_eu_bq()
        chromeOptions = webdriver.ChromeOptions()
        driver_path = '/usr/local/bin/chromedriver'
        chromeOptions.add_argument('--headless')
        chromeOptions.add_argument('--disable-gpu')
        chromeOptions.add_argument('--no-sandbox')

    
        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), chrome_options=chromeOptions)
        url = "https://www.enforcementtracker.com/"
        driver.get(url)
        driver.implicitly_wait(10)
        button_100 = driver.find_element(By.XPATH, '/html/body/div/div/div/div[6]/div[1]/label/select/option[4]')
        button_100.click()
        time.sleep(5)
        columns = []
        thead = driver.find_element(By.TAG_NAME, 'thead')
        headers = thead.find_elements(By.TAG_NAME, 'th')
        button_sup = driver.find_element(By.XPATH,"/html/body/div/div/div/div[6]/table/tbody/tr[1]/td[1]")
        button_sup.click()
        child = driver.find_element(By.CLASS_NAME,"child")
        headers_sup = child.find_elements(By.CLASS_NAME, "dtr-title")
        for column in headers:
            columns.append(column.text)
        for column in headers_sup:
            columns.append(column.text)
        button_sup.click()
        driver.execute_script("window.scrollBy(0, 150);")
        columns = [element for element in columns if element.strip() != '']
        content = driver.find_element(By.TAG_NAME, 'tbody')
        rows = content.find_elements(By.TAG_NAME,'tr')
        all_content = []
        counter_scroll = 0
        found_update = False
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, 'table#penalties tbody tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                cells_content = []
                if self.last_sanc_eu in cells[1].text:
                    found_update = True
                    break
                else:
                    for cell in cells:
                        cells_content.append(cell.text)
                    
                    try:
                        href_element = cells[11].find_element(By.TAG_NAME, 'a')
                        href = href_element.get_attribute('href')
                    except NoSuchElementException:
                        href = 'None'
                    
                    cells_content.append(href)
                    cells[0].click()
                    child = driver.find_element(By.CLASS_NAME, "child")
                    data_sup = child.find_elements(By.CLASS_NAME, "dtr-data")
                    for cell in data_sup:
                        cells_content.append(cell.text)

                    cells_content = [element for element in cells_content if element.strip() != '']
                    all_content.append(cells_content)
                    cells[0].click()
                    cells_content.pop(7)
                    counter_scroll += 10
                    driver.execute_script("window.scrollBy(0, 150);")

            next_page = driver.find_element(By.ID, 'penalties_next')
            if "disabled" in next_page.get_attribute('class'):
                break
            elif found_update:
                break   # Exit the loop if the "Next" button is disabled
            else:
                driver.execute_script("arguments[0].scrollIntoView();", next_page)
                next_page.click()
                # Wait for the page to load, if needed
                # Add any necessary sleep or wait statements here if the next page load takes some time
        driver.close()
        self.df_sanc_eu_updt = pd.DataFrame(data=all_content, columns=columns)
        return self.df_sanc_eu_updt

    def get_sanctions_eu_bq(self):
        query_string = f'SELECT * FROM `cnil-392113.sanctions_eu.sanctions_all_eu`'
        self.df_sanctions_eu_bq = pd.read_gbq(query_string, credentials=self.credentials)
        self.df_sanctions_eu_bq['etid_number'] = self.df_sanctions_eu_bq['etid'].str.extract('(\d+)', expand=False).astype(int)
        self.df_sanctions_eu_bq.sort_values(by=['etid_number'], ascending=False, inplace=True)
        self.last_sanc_eu = self.df_sanctions_eu_bq.iloc[0]['etid_number'].astype(str)
        return self.last_sanc_eu

    def concat_new_sanctions_eu(self):

        self.df_sanc_eu_updt.rename(columns={
            'ETid': 'etid',
            'Country': 'country',
            'Date of Decision': 'date_of_decision',
            'Fine [€]': 'fine_eur',
            'Controller/Processor': 'controllerprocessor',
            'Quoted Art.': 'quoted_art',
            'Type': 'type',
            'Source': 'source',
            'Authority': 'authority',
            'Sector': 'sector',
            'Summary': 'summary',
            'Direct URL': 'direct_url'
        }, inplace=True)


        self.df_sanc_eu_updt['etid_number'] = self.df_sanc_eu_updt['etid'].str.extract('(\d+)', expand=False).astype(int)
        self.df_sanc_eu_updt.sort_values(by=['etid_number'], ascending=False, inplace=True)
        self.concat_df_sanc_eu = pd.concat([self.df_sanctions_eu_bq, self.df_sanc_eu_updt], axis=0, ignore_index=True)
        self.concat_df_sanc_eu = self.concat_df_sanc_eu.drop_duplicates()
        self.concat_df_sanc_eu.sort_values(by=['etid_number'], ascending=False, inplace=True)
        return self.concat_df_sanc_eu
    
    def save_sanc_eu_csv(self):
        dataset = 'sanctions_eu'
        table = 'sanctions_all_eu'
        self.dataset_sanc_eu = dataset
        self.table_sanc_eu = table
        os.makedirs('datasets/sanctions_eu', exist_ok=True)
        self.concat_df_sanc_eu.to_excel('datasets/sanctions_eu/sanctions_all_eu', engine='openpyxl')
        print('CSV file saved successfully')


