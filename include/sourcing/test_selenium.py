def test_selenium():
  from selenium import webdriver
  options = webdriver.ChromeOptions()
  options.add_argument('--ignore-ssl-errors=yes')
  options.add_argument('--ignore-certificate-errors')

  user_agent = 'userMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
  options.add_argument(f'user-agent={user_agent}')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument('--no-sandbox')
  options.add_argument('-headless')
  remote_webdriver = 'remote_chromedriver'
  with webdriver.Remote(f'http://localhost:4444/wd/hub', options=options) as driver:
    url = "https://www.enforcementtracker.com/"
    driver.get(url)
    driver.implicitly_wait(10)
    driver.quit()
