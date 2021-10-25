from bs4 import BeautifulSoup
import requests
import pandas
from datetime import date
from datetime import datetime
import boto3


s3 = boto3.resource('s3')

def handler(event, context):
    print("hello from zappa ii")
    url = "https://www.eltiempo.com/"
    url2 = "https://www.elespectador.com/"

    #Día actual
    #today = date.today()

    #Fecha actual
    #now = datetime.now()

    #dia = today.day
    #mes= today.month
    #año = today.year 

    page = requests.get(url)
    page2 = requests.get(url2)

    soup = BeautifulSoup(page.content, 'html.parser')
    soup2 = BeautifulSoup(page.content, 'html.parser')

    fecha = soup2.find('div', class_='fecha').getText()
    
    dia = fecha[16:19]
    año = '2021'
    mes = '10'

    def eltiempo():
        htmlpage= open('/tmp/eltiempo.html', 'w')
        htmlpage.write(str(soup))
        htmlpage.close()

    def elespectador():
        htmlpage= open('/tmp/elespectador.html', 'w')
        htmlpage.write(str(soup))
        htmlpage.close()

    eltiempo()
    elespectador()

    s3.meta.client.upload_file('/tmp/eltiempo.html', 'miperiodico007',
                                f'headlines/raw/periodico=ElTiempo/year={año}/month={mes}/day={dia}/eltiempo.html')
                                
                    
    s3.meta.client.upload_file('/tmp/elespectador.html', 'miperiodico007',
                                f'headlines/raw/periodico=ElEspectador/year={año}/month={mes}/day={dia}/elespectador.html')                
    return{
        "Status": 200
    }                               