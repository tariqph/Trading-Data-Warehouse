import logging
from kiteconnect import KiteConnect
from configparser import ConfigParser
from requests.api import request
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
import time
import pyotp


logging.basicConfig(level=logging.DEBUG)

url = "https://kite.trade/connect/login?api_key="

filename = 'database.ini'
section = 'zerodha'

parser = ConfigParser()
# read config file
parser.read(filename)

db = {}
if parser.has_section(section):
	params = parser.items(section)
	for param in params:
		db[param[0]] = param[1]


# print(url + db['api_key'])
url = url + db['api_key']

driver = webdriver.Chrome()
driver.get(url) 
action = ActionChains(driver)

time.sleep(2)

user_id = driver.find_element_by_id('userid')
print('here')
password= driver.find_element_by_id('password')
login = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[4]/button')

user_id.send_keys(db['user_id'])
password.send_keys(db['password'])
login.click()
time.sleep(2)

# print(db['totp_key'])
totp = pyotp.TOTP(db['totp_key'])


pin = driver.find_element_by_id('totp')
cont = driver.find_element_by_xpath('/html/body/div[1]/div/div[2]/div[1]/div/div/div[2]/form/div[3]/button')
print(totp.now())
pin.send_keys(totp.now())
cont.click()

time.sleep(2)

curr_url = driver.current_url
curr_url = curr_url.split('&')
print('url', curr_url)

token = ''
for string in curr_url:
    if 'request_token' in string:
        token = string

token = token.split('=')
request_token = token[1]
print(request_token)

api_key = db['api_key']
api_secret=db['api_secret']

kite = KiteConnect(api_key=api_key)

data = kite.generate_session(request_token, api_secret=api_secret)
print(data['access_token'])
parser.set('zerodha','access_token', data['access_token'])

with open('database.ini', 'w') as configfile:
    parser.write(configfile)
    

driver.quit()