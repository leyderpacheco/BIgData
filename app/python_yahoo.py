import requests
import datetime
import csv
from bs4 import BeautifulSoup
import pandas as pd


def scraper1():
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1=1634428800&period2=1634601600&interval=1d&events=history&includeAdjustedClose=true"
        df = pd.Dataframe(url)
        print(df)
    except Exception as e:
        print("nani")


scraper1()
