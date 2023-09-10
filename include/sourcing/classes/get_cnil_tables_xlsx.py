import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

class GetCnilTablesCSV:

    def __init__(self):
        self.df_indexes = pd.read_csv('metadata/cnil_dataset_metadata.csv')
        self.get_tables()
        self.get_sanctions_tables()

    def get_tables(self):
        for index, row in self.df_indexes.iterrows():
            os.makedirs(f'datasets/{row.slug}', exist_ok=True)
            df = pd.read_excel(row.download_url, engine='openpyxl')
            df.to_excel(f'datasets/{row.slug}/{row.title}', engine='openpyxl')

    def get_sanctions_tables(self):
        self.url_sanctions = 'https://www.cnil.fr/fr/les-sanctions-prononcees-par-la-cnil'
        self.dfs_sanctions = pd.read_html(self.url_sanctions)
        self.get_sanctions_link()
        self.incorate_sanctions_link()
        self.incorate_year_and_save()

    def get_sanctions_link(self):
        soup = BeautifulSoup(requests.get(
            self.url_sanctions).content, 'html.parser')
        tbody_list = soup.find_all('tbody')

        self.all_links = []

        for tbody in tbody_list:
            table_links = []
            rows = tbody.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if cells and cells[-1].find(['a']):
                    link = cells[-1].find(['a']).get('href')
                    table_links.append(link)
                else:
                    link = 'No link'
                    table_links.append(link)
            self.all_links.append(table_links)

    def incorate_sanctions_link(self):
        for index, df in enumerate(self.dfs_sanctions):
            try:
                df['lien_vers_la_decision'] = self.all_links[index]
            except ValueError:
                df['lien_vers_la_decision'] = self.all_links[index][1:]
    
    def incorate_year_and_save(self):
        for index, df in enumerate(self.dfs_sanctions):
            year = 2023 - index
            self.dfs_sanctions[index]['year'] = year
            df.to_excel(
                    f'datasets/sanctions_prononcees_par_la_cnil/listes_sanctions_{year}', engine='openpyxl')
            
    def get_sanctions_eu(self):
    # Replace the path with the location of your webdriver executable (e.g., chromedriver, geckodriver)
        driver = webdriver.Firefox()

        # URL of the page containing the table
        url = "https://www.enforcementtracker.com/"

        driver.get(url)

        # Wait for the table to load (you may need to adjust the wait time based on the page loading speed)
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
        print(columns)

        content = driver.find_element(By.TAG_NAME, 'tbody')
        rows = content.find_elements(By.TAG_NAME,'tr')
        print(len(rows))

        all_content = []
        counter_scroll = 0
        while True:
            rows = driver.find_elements(By.CSS_SELECTOR, 'table#penalties tbody tr')
            
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                cells_content = []
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
                break  # Exit the loop if the "Next" button is disabled
            else:
                driver.execute_script("arguments[0].scrollIntoView();", next_page)
                next_page.click()
                # Wait for the page to load, if needed
                # Add any necessary sleep or wait statements here if the next page load takes some time

        print(all_content)
        driver.close()
        df = pd.DataFrame(data=all_content, columns=columns)
        os.makedirs(f'datasets/sanctions_eu', exist_ok=True)
        df.to_excel(f'datasets/sanctions_eu/sanctions_all_eu', engine='openpyxl')

class UpdateTableCSV:

    def __init__(self, dataset, table,link):
        self.download_url = link
        self.dataset = dataset
        self.table = table
        self.get_table()

    def get_table(self):
        os.makedirs(f'datasets/{self.dataset}', exist_ok=True)
        df = pd.read_excel(self.download_url, engine='openpyxl')
        print(f'Le fichier datasets/{self.dataset}/{self.table} a été mis à jour')
        df.to_excel(f'datasets/{self.dataset}/{self.table}', engine='openpyxl')
        return df
    