#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

def run_api():
    global df
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest' 
    parameters = {
      'start':'1',
      'limit':'100',
      'convert':'USD'
    }
    headers = {
      'Accepts': 'application/json',
          'X-CMC_PRO_API_KEY': 'ae69b8ff-5418-4ce1-8908-a1d9ec1cdbb8',
    }

    session = Session()
    session.headers.update(headers)

    try:
      response = session.get(url, params=parameters)
      data = json.loads(response.text)
      df = pd.json_normalize(data['data'])
      pd.set_option('display.max_columns', None)
      pd.set_option('display.max_rows', None)
      df['timestamp'] = pd.to_datetime('now')
      #print(data)

    except (ConnectionError, Timeout, TooManyRedirects) as e:
      print(e)
    

run_api()


df.to_csv('version1.csv')


# In[ ]:





# In[ ]:





# In[ ]:




