

import requests
import pprint

from bs4 import BeautifulSoup
from pymongo import MongoClient

from datetime import datetime
from calendar import monthrange
import time
import random

import uuid
from tqdm import tqdm
    
import credentials
    
    

def get_url(city: str, loc_key: str, day: int):
    '''
    Function takes in a city name, a location key and a day value.
    
    City name and location key are as defined by accuweather. values can be
    obtained via accuweather's location API:
    
    http://dataservice.accuweather.com/locations/v1/cities/autocomplete.
    
    Otherwise, make use of search bar at https://www.accuweather.com/ to get city name
    and location key as presented by the resulting url: 
    
    https://www.accuweather.com/en/my/{city_name}/228029/weather-forecast/{location key}
    
    Function returns a url to query.
    
    -----
    Parameters:
    
    city:       City name in string format.
    loc_key:    Location key
    day:        Integer value from min of 1 to max of 91. 1 corresponds to the present day
                and 91 corresponds to the 91st day forecast, i.e. 90 days in the future.
    
    
    Returns:
    
    url:        'https://www.accuweather.com/en/sg/{city}/{loc_key}/daily-weather-forecast/
                 {loc_key}?day={day}'
    
    -----
    
    '''   
    
    server = 'https://www.accuweather.com/en/sg/'
    
    city = str(city).lower().replace(' ', '-')
    
    loc_key = str(loc_key)

    url = server + f'{city}/{loc_key}/daily-weather-forecast/{loc_key}?day={day}'    
    
    return url


def get_soup(url):
    '''
    Function takes in a url in string format and returns a BeautifulSoup if response status
    code = 200 (successful).
    
    Function returns response header otherwise.
    '''
    import requests
    from bs4 import BeautifulSoup
    
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36'}
    
    resp = requests.get(url, headers = headers)

    if resp.status_code == 200:

        soup = BeautifulSoup(resp.text, features='lxml')

        return soup

    else:
        print('Error: ' + f'{resp.status_code}' + "\n" + f'{resp.headers}')
    
    
    
    
def get_tag_sib_text(tag):
    '''
    Function that takes in a single BeautifulSoup tag and will find its next siblings and 
    return the text in the current tag and its sibling tags as strings in a list.
    
    -----
    Parameters:
    
    tag:      bs4.element.Tag    
    
    
    Returns:
    
    texts:    ['text in current tag', 'text in first sibling tag', 
               'text in second sibling tag', ... ]
    
    -----
    '''
    
    # get current tag text
    curr_tag_text = tag.text
        
    # find next siblings of tags, returns a list
    sibling_tags = tag.find_next_siblings()
        
    # get text from siblings
    texts = [s.text for s in sibling_tags]
    
    # add tag_text into list at index 0
    texts.insert(0, curr_tag_text)
    
    return texts




def get_day_record(soup):
    '''
    Function takes in a BeautifulSoup, searches for relevant tags containing temperature
    information and creates a dictionary containing these temperature data.
    
    Function calls upon the get_tag_sib_text() function.
    
    Function returns a dictionary.
    
    -----
    Parameters:
    
    soup:   bs4.BeautifulSoup
    
    
    Returns:
    
    day_record:    {'day':            integer value from 1 to 31,
                    'month':          integer value from 1 to 12,
                    'year':           integer value,
                    'forecast_high':  temperature in string format,
                    'forecast_low':   temperature in string format,
                    'last_year_high': temperature in string format,
                    'last_year_low':  temperature in string format,
                    'hist_avg_high':  temperature in string format,
                    'hist_avg_low:    temperature in string format
                    }
    
    -----
    
    ''' 
    from datetime import datetime
   
    day_record = {}
    
    # in the daily weather forecast view, the temperature history is found within the
    # <div class="temp-history content-module">
    tags = soup.find_all(attrs = {'class':['temp-history']})[0]
    
    # to access the dates
    date = tags.find('span', {'class':'header-date'}).text.split('/')
    day_record['day'] = int(date[1])
    day_record['month'] = int(date[0])
    
    # get year
    curr_date = datetime.now()
    
    if day_record['month'] < curr_date.month:
        y_ = curr_date.year + 1
    elif day_record['month'] == curr_date.month:
        if day_record['day'] < curr_date.day:
            y_ = curr_date.year + 1
        else:
            y_ = curr_date.year
    else:
        y_ = curr_date.year
        
    day_record['year'] = y_

    # find all temperature tags
    temp_tags = tags.find_all(attrs={'class': 'label'})
    
    # get temperatures within relevant temperature tags
    for temp_tag in temp_tags:
        if temp_tag.text == 'Forecast':
            forecast = get_tag_sib_text(temp_tag)
        if temp_tag.text == 'Average':
            hist_avg = get_tag_sib_text(temp_tag)
        if temp_tag.text == 'Last Year':
            last_year = get_tag_sib_text(temp_tag)
    
    # assigning temp values to dict
    day_record['forecast_high'] = forecast[1].replace('°', '')
    day_record['forecast_low'] = forecast[2].replace('°', '')
    
    day_record['hist_avg_high'] = hist_avg[1].replace('°', '')
    day_record['hist_avg_low'] = hist_avg[2].replace('°', '')
    
    day_record['last_year_high'] = last_year[1].replace('°', '')
    day_record['last_year_low'] = last_year[2].replace('°', '')

    return day_record
    




def create_day_record_doc(city: str, loc_key: str, snapshot_dt, day_record):
    '''
    Function takes in a user-defined city, loc_key, snapshot_dt. It also takes 
    in a day_record dictionary.
    
    Function then creates and return a dictionary.
    
    -----
    Parameters:
    
    city:              User-input city name in string format
    loc_key:           User-input location key in string format
    snapshot_dt:       snapshot datetime object
    day_record:        Dictionary as returned by get_day_record()
    
    
    Returns:
    
    day_record_doc:    {'day_record_id':      some hash value,
                        'city' :              city name in string,
                        'loc_key':            location key in string,
                        'snapshot_dt' :       snapshot datetime,
                        'cal_day' :           day_record['day'],
                        'cal_month':          day_record['month'],
                        'cal_year':           day_record['year'],
                        'temp_info': {
                            'forecast_high':  temperature in string format,
                            'forecast_low':   temperature in string format,
                            'last_year_high': temperature in string format,
                            'last_year_low':  temperature in string format,
                            'hist_avg_high':  temperature in string format,
                            'hist_avg_low:    temperature in string format
                            }
                        }
    
    -----
    
    ''' 
        
    import uuid
    
    day_record_doc = {
        'day_record_id': str(uuid.uuid1()),
        'city' : str(city).lower().replace(' ', '-'),
        'loc_key': str(loc_key),
        'snapshot_dt' : snapshot_dt,
        'cal_day' : day_record['day'],
        'cal_month': day_record['month'],
        'cal_year': day_record['year'],
        'temp_info' : {
            'forecast_high' : day_record['forecast_high'],
            'forecast_low' : day_record['forecast_low'],
            'last_year_high' : day_record['last_year_high'],
            'last_year_low' : day_record['last_year_low'],
            'hist_avg_high': day_record['hist_avg_high'],
            'hist_avg_low': day_record['hist_avg_low']

        }
    }
    return day_record_doc
                 
    

    
    
def connect_coll(coll_name:str):
    '''
    Function takes in a user-defined collection name and gets that collection within the
    'temp_records' database. If the collection or the database does not exist, it will create
    them when user inserts a document using the returned pymongo.collection.Collection object.
        
    Function returns a pymongo.collection.Collection object.
    
    -----
    Parameters:
    
    coll_name:  User-input collection name in string format
    
    
    Returns:
    
    coll:       pymongo.collection.Collection object
    
    -----
    '''
    
    from pymongo import MongoClient
    
    user = credentials.db_username
    pw = credentials.db_pw
    db_name = 'temp_records'
    
    url = f'mongodb+srv://{user}:{pw}@cluster0.0zyjn.mongodb.net/{db_name}?retryWrites=true&w=majority'
    client = MongoClient(url)
    
    # url = 'mongodb://127.0.0.1:27017'
    # client = MongoClient(url)
    # db = client.get_database('temp_records')
    db = client[db_name]
    
    coll = db.get_collection(str(coll_name))
    
    return coll




def take_snapshot(locations_dict, delay=1):
    '''
    Function takes in a dictionary that contains the location keys and city names of interest
    and the function will take a snapshot.

    Function also takes in a delay variable to delay the scraping per url call.
    
    A snapshot would generate a snapshot_doc with a unique id, a series of loc_doc for each new
    location that is not present in the database collection: 'temp_records'.'loc_collection', and
    a series of day_record_doc for each unique location key and city names in the input dictionary.
    
    Funtion calls upon the following functions:
        - connect_coll()
        - create_day_record_doc()
        - get_url()
        - get_soup()
        - 
        
    Function then uploads the snapshot_doc, the cal_day_doc (if any), the day_record_doc into the 
    relevant pymongo.collection.Collection.
    
    -----
    Parameters:
    
    loations_dict:  User-input dictionary name - {'228029': 'Johor Bahru',
                                                  'xxxxxx': 'YYYYY YYY',
                                                  ...}
    delay - Int: Delay variable per url request
    
    Returns:
    
    None
    
    -----
    '''
    
    from tqdm import tqdm
    from datetime import datetime
    import time
    import random
    # 
    
    # establish connection to collections
    day_record_coll = connect_coll('day_record_collection')
    # day_record_coll = connect_coll('test')
    # create snapshot datetime
    snapshot_dt = datetime.utcnow()
    
    day_record_doc_list = []
   
    
    for loc_key, city in locations_dict.items():              

        # get records from day 1(present) to day 91(90 days in future)
        for day in tqdm(range(1, 92)):
            url = get_url(city,loc_key,day)
            soup = get_soup(url)
            day_record = get_day_record(soup)

            # create day_record_doc and append to list
            day_record_doc = create_day_record_doc(city, loc_key, snapshot_dt, day_record)
            day_record_doc_list.append(day_record_doc)
            
            # add timeout
            time.sleep(random.uniform(1, 2) * delay)
            
    # upload documents   
    day_record_coll.insert_many(day_record_doc_list)
    
    print('Task Completed')
    
    return




def get_loc_key(city_name: str):
    '''
    Function takes in a city name of interest in string format and paasses this through
    theuse accuweather autocomplete api to get location key for subsequent scrapping.
    
    Note that API request limited to 50 per day. Function prints out the remaining calls
    for the day.
    
    Function imports credentials.py which contains the apikey
    
    Function returns a list of dictionary containing probable matching location name and
    location key.
    
        
    '''
    import credentials
    import pprint

    apikey = credentials.aw_apikey
    url = 'http://dataservice.accuweather.com/locations/v1/cities/autocomplete'
    
    resp = requests.get(url, params= {'apikey':apikey, 'q':str(city_name).lower()})
    if resp.status_code != 200:
        print('Please check the location input')
    else:
        pprint.pprint(resp.json())

    print(resp.headers['RateLimit-Remaining'])