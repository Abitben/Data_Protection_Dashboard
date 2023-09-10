import os
import pandas as pd
import requests
import time
import datetime
import re
from unidecode import unidecode
from bs4 import BeautifulSoup
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas_gbq
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC