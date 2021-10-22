import json
import boto3
import csv
import urllib.request
import pandas as pd
from io import StringIO

s3 = boto3.resource('s3')
bucket = s3.Bucket('miyahoo007')


def handler(event, context):
    print("hello from zappa")

    # bucket = event['Records'][0]['s3']['bucket']['name'] # Will be `my-bucket`
    # key = event['Records'][0]['s3']['object']['key'] # Will be the file path of whatever file was uploaded.

    # print(bucket)
    # print(key)
    print(event)
    url1 = "https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true"
    url2 = "https://query1.finance.yahoo.com/v7/finance/download/EC?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true"
    url3 = "https://query1.finance.yahoo.com/v7/finance/download/AVAL?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true"
    url4 = "https://query1.finance.yahoo.com/v7/finance/download/CMTOY?period1=1634774400&period2=1634860800&interval=1d&events=history&includeAdjustedClose=true"

    key = 'AVHOQ.csv'
    key1 = 'EC.csv'
    key2 = 'AVAL.csv'
    key3 = 'CMTOY.csv'


    def data1():
        respuesta = urllib.request.urlopen(url1)
        f = StringIO(bytearray(respuesta.read()).decode())
        archivo = csv.reader(f)


        with open('/tmp/AVHOQ.csv', 'w') as f:
            writer = csv.writer(f)
            for line in archivo:
                writer.writerow(line)

    def data2():
        respuesta = urllib.request.urlopen(url2)
        f = StringIO(bytearray(respuesta.read()).decode())
        archivo = csv.reader(f)

        with open('/tmp/EC.csv', 'w') as f:
            writer = csv.writer(f)
            for line in archivo:
                writer.writerow(line)

    def data3():
        respuesta = urllib.request.urlopen(url3)
        f = StringIO(bytearray(respuesta.read()).decode())
        archivo = csv.reader(f)

        with open('/tmp/AVAL.csv', 'w') as f:
            writer = csv.writer(f)
            for line in archivo:
                writer.writerow(line)

    def data4():
        respuesta = urllib.request.urlopen(url4)
        f = StringIO(bytearray(respuesta.read()).decode())
        archivo = csv.reader(f)

        with open('/tmp/CMTOY.csv', 'w') as f:
            writer = csv.writer(f)
            for line in archivo:
                writer.writerow(line)
    data1()
    data2()
    data3()
    data4()
    #respuesta = urllib.request.urlopen(url1)
    #respuesta1 = urllib.request.urlopen(url2)
    #respuesta2 = urllib.request.urlopen(url3)
    #respuesta3 = urllib.request.urlopen(url4)

    #f = StringIO(bytearray(respuesta.read()).decode())
    #f1 = StringIO(bytearray(respuesta1.read()).decode())
    #f2 = StringIO(bytearray(respuesta2.read()).decode())
    #f3 = StringIO(bytearray(respuesta3.read()).decode())

    #archivo = csv.reader(f)
    #archivo1 = csv.reader(f1)
    #archivo2 = csv.reader(f2)
    #archivo3 = csv.reader(f3)

    #with open('/tmp/AVHOQ.csv', 'w') as f:
        #writer = csv.writer(f)
        #for line in archivo:
            #writer.writerow(line)

    #with open('/tmp/EC.csv', 'w') as f1:
        #writer1 = csv.writer(f1)
        #for line in archivo1:
            #writer1.writerow(line)

    #with open('/tmp/AVAL.csv', 'w') as f2:
        #writer2 = csv.writer(f2)
        #for line in archivo2:
            #writer2.writerow(line)  

    #with open('/tmp/CMTOY.csv', 'w') as f3:
        #write3r = csv.writer(f3)
        #for line in archivo3:
            #writer3.writerow(line)  

    data1 = pd.read_csv((url1))
    data2 = pd.read_csv(url2)
    data3 = pd.read_csv(url3)
    data4 = pd.read_csv(url4)

    #temp_csv_file = csv.writer(open("/tmp/data1.csv", "w+"))
    # temp_csv_file.writerow(data1)
    #temp_reader = csv.DictReader(temp)
    #ifile  = open('/tmp/data1.csv', "r")
    #ifile1  = open('/tmp/data2.csv', "r")
    #ifile2  = open('/tmp/data3.csv', "r")
    #ifile3  = open('/tmp/data4.csv', "r")

    #read = csv.reader(ifile)
    #read1 = csv.reader(ifile1)
    #read2 = csv.reader(ifile2)
    #read2 = csv.reader(ifile3)

    año = data1['Date'][0][0:4]
    mes = data1['Date'][0][5:7]
    dia = data1['Date'][0][8:10]

    bucket1 = 'miyahoo007'
    bucket2 = 'miyahoo007/stocks/company=EC/year={año}/month={mes}/day={dia}'
    bucket3 = 'miyahoo007/stocks/company=AVAL/year={año}/month={mes}/day={dia}'
    bucket4 = 'miyahoo007/stocks/company=CMTOY/year={año}/month={mes}/day={dia}'

    s3.meta.client.upload_file('/tmp/AVHOQ.csv', 'miyahoo007',
                               f'stocks/company=AVHOQ/year={año}/month={mes}/day={dia}/{key}')
    s3.meta.client.upload_file('/tmp/EC.csv', 'miyahoo007',
                               f'stocks/company=EC/year={año}/month={mes}/day={dia}/{key1}')
    s3.meta.client.upload_file('/tmp/AVAL.csv', 'miyahoo007',
                               f'stocks/company=AVAL/year={año}/month={mes}/day={dia}/{key2}')
    s3.meta.client.upload_file('/tmp/CMTOY.csv', 'miyahoo007',
                               f'stocks/company=CMTOY/year={año}/month={mes}/day={dia}/{key3}')                           
    #s3.meta.client.upload_file(Filename = '/tmp/data2.csv', Bucket= bucket1, Key = '/stocks/company=EC/year={año}/month={mes}/day={dia}/data2.csv')
    #s3.meta.client.upload_file(Filename = '/tmp/data3.csv', Bucket= bucket1, Key = '/stocks/company=AVAL/year={año}/month={mes}/day={dia}/data3.csv')
    #s3.meta.client.upload_file(Filename = '/tmp/data4.csv', Bucket= bucket1, Key = '/stocks/company=CMTOY/year={año}/month={mes}/day={dia}/data4.csv')

    return{
        "Status": 200
    }
