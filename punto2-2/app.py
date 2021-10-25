import json
import boto3
from datetime import date
from datetime import datetime
from bs4 import BeautifulSoup
import re
import urllib3
import pandas as pd
import csv

#Día actual
today = date.today()

    #Fecha actual
now = datetime.now()

dia = today.day
mes= today.month
año = today.year

def handler(event, context):
    # TODO implement
    
    print('archivo recibido')
    
    s31 = boto3.client('s3')
    s3 = boto3.resource('s3')

    bucket = 'miperiodico007'
    #bucket = event['Records'][0]['s3']['bucket']['name']
    #key = event['Records'][0]['s3']['object']['key']
    #key = 'eltiempo.html'
    
    print(bucket)
    #print(key)
    
    #s3.Bucket(bucket).download_file(key, '/tmp/tiempo.html')
    s31.download_file(bucket, f'headlines/raw/periodico=ElTiempo/year={año}/month={mes}/day={dia}/eltiempo.html', '/tmp/tiempito.html')
    s31.download_file(bucket, f'headlines/raw/periodico=ElEspectador/year={año}/month={mes}/day={dia}/elespectador.html', '/tmp/espectadorcito.html')
    
    url = '/tmp/tiempito.html'
    url1 = '/tmp/espectadorcito.html'

    with open('/tmp/tiempito.html') as fp:
        soup = BeautifulSoup(fp, 'html.parser')

    with open('/tmp/espectadorcito.html') as fp1:
        soup1 = BeautifulSoup(fp1, 'html.parser')

    def tiempo():


        cat = soup.find_all('div', {'class': 'category-published'})
        categorias = list()
        for i in cat:
            categorias.append(i.text)

        tit = soup.find_all('a', {'class': 'title page-link'})
        titulares = list()

        for i in tit:
            titulares.append(i.text)

        links = list()

        for a in soup.find_all('a',{'class': 'multimediatag page-link'}, href=True): 
            if a.text: 
                links.append('https://www.eltiempo.com'+a['href'])

        #df = pd.DataFrame({'Categorias':categorias, 'Titulares':titulares, 'Link':links})
        a = {'Categorias':categorias, 'Titulares':titulares, 'Link':links}
        df = pd.DataFrame.from_dict(a,orient='index')


        #with open('/tmp/Eltiempo.csv', 'w') as f:
            #writer = csv.writer(f)
            #for line in df:
                #writer.writerow(line)

        df.to_csv('/tmp/Eltiempo.csv', index=False)

    def espectador():

        

        cat = soup1.find_all('h4', {'class': 'Card-Section Section'})
        categorias = list()
        for i in cat:
            categorias.append(i.text)

        tit = soup1.find_all('h2', {'class': 'Card-Title Title Title'})
        titulares = list()
        for i in tit:
            titulares.append(i.text)

        link = soup1.find_all('h2', {'class': 'Card-Title Title Title'})
        links = list()

        for i in link:
            for a in i.find_all('a', href=True):
                links.append('https://www.elespectador.com/'+a['href'])


        a = {'Categorias':categorias, 'Titulares':titulares, 'Link':links}
        df = pd.DataFrame.from_dict(a,orient='index')


        with open('/tmp/Elespectador.csv', 'w') as f:
            writer = csv.writer(f)
            for line in df:
                writer.writerow(line)

        df.to_csv('/tmp/Elespectador.csv', index=False)
    tiempo()
    espectador()


    s3.meta.client.upload_file('/tmp/Eltiempo.csv', 'periodico007results',
                               f'news/final/periodico=Tiempo/year={año}/month={mes}/day={dia}/Eltiempo.csv')
                               
    s3.meta.client.upload_file('/tmp/Elespectador.csv', 'periodico007results',
                               f'news/final/periodico=Espectador/year={año}/month={mes}/day={dia}/Elespectador.csv')
    #print(open('/tmp/tiempito.html').read())
    #s3.meta.client.download_file(bucket, key , 'tiempo.html')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }