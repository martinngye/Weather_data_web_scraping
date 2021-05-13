# Weather Data Web Scraping

This repo contains work related to a weather data web scraper using python and beautifulSoup4. This project was carried out a a request for a friend.

The scripts scrape AccuWeather for the daily snapshot of the forecasted, past and historical average high and low temperatures of a user-defined list and stores the data on MongoDB. These scripts were developed for learning/educational purposes and I hope that they will be used for non-commerical purposes only.

All related scripts are in the [Scraper](/Scraper) folder:
* credentials.py - to contain credentials for MongoDB database, and AccuWeather API key
* my_functions.py - contains the various dependency functions that will be used in default_start.py
* default_start.py - the initialisation script, containing the user-defined list of cities to scrape.
* scrape_accuweather.bat - batch file to run the default_start.py. May be used to schedule the job in Windows

Refer to [project notebook](martin_ng_project.ipynb) for more information on how to run the scraper.
