# Jinesh Modi - 2464
# CS-6331-001
# Assignment3

from flask import Flask, render_template, request
from datetime import datetime
from json import loads, dumps
# import pypyodbc
import sqlite3
import os
# import pandas as pd
import random
import time
import redis
import hashlib

# from werkzeug.utils import secure_filename

app = Flask(_name_, template_folder='static')
port = os.getenv('PORT', '10000')

server = 'jinesh-assignment3.database.windows.net'
database = 'cloud'
username = 'jinesh'
password = 'J!n!@3594'
driver= '{ODBC Driver 13 for SQL Server}'
# cnxn = pypyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)

myHostname = "jinesh-cloud.redis.cache.windows.net"
myPassword = "6ZYqOPOHM465LtMEZpES5ctQkEU7SNzMOBCM6qKPpHw="

# r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)

@app.route('/')
def hello_world():
    # conn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    conn = sqlite3.connect("quakes")
    lstDictionaryData = []
    cursor = conn.cursor()
    startTime = time.time()
    query = "SELECT TOP 10 latitude, longitude, mag, place FROM all_month"
    # print(query)
    cursor.execute(query)
    # print(tmp)
    endTime = time.time()
    row = cursor.fetchone()
    while row:
        lstDictionaryData.append(row)
        # print("hi!" + str(row))
        row = cursor.fetchone()
    # return "hello!!"
    conn.close()
    executionTime = (endTime - startTime) * 1000
    return render_template('index.html', tableData=lstDictionaryData, tableDataLen=lstDictionaryData._len_(), executionTime=executionTime)

@app.route('/createtable',methods=['GET', 'POST'])
def createTable():
    lstDictionaryData = []
    # conn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    conn = sqlite3.connect("quakes")
    cursor = conn.cursor()
    # query = "CREATE TABLE dbo.all_month (\"time\" datetime, \"latitude\" FLOAT, \"longitude\" FLOAT, \"depth\" FLOAT, \"mag\" FLOAT, \"magType\" TEXT, \"nst\" INT, \"gap\" INT, \"dmin\" FLOAT, \"rms\" FLOAT, \"net\" TEXT, \"id\" TEXT, \"updated\" datetime, \"place\" TEXT, \"type\" TEXT, \"horontalError\" FLOAT, \"depthError\" FLOAT, \"magError\" FLOAT, \"magNst\" INT, \"status\" TEXT, \"locationSource\" TEXT, \"magSource\" TEXT)"
    query = "CREATE TABLE cloudsqldb.dbo.all_month(time DATETIME,latitude FLOAT,longitude FLOAT,depth FLOAT,mag FLOAT,magType TEXT,nst INT,gap INT,dmin FLOAT,rms FLOAT,net TEXT,id TEXT,updated DATETIME,place TEXT,type TEXT,horontalError FLOAT,depthError FLOAT,magError FLOAT,magNst INT,status TEXT,locationSource TEXT,magSource TEXT)"
    # print(query)
    startTime = time.time()
    # cursor.execute(query)
    cursor.execdirect(query)
    cursor.execdirect("CREATE INDEX all_month_mag__index ON cloudsqldb.dbo.earthquake (mag)")
    cursor.execdirect("CREATE INDEX all_month_lat__index ON cloudsqldb.dbo.earthquake (latitude)")
    cursor.execdirect("CREATE INDEX all_month_long__index ON cloudsqldb.dbo.earthquake (longitude)")
    endTime = time.time()
    conn.close()
    executionTime = (endTime - startTime) * 1000
    return render_template('index.html', tableData=lstDictionaryData, tableDataLen=lstDictionaryData._len_(), executionTime=executionTime)

@app.route('/randomqueries', methods=['GET', 'POST'])
def randomQueries():
    noOfQueries = int(request.form('queries', ''))
    withCache = int(request.form('withCache', ''))
    magnitudeStart = float(request.form('magnitudeStart', ''))
    magnitudeEnd = float(request.form('magnitudeEnd', ''))

    lstDictionaryData = []
    lstDictionaryDataDisplay = []

    # conn = pypyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
    conn = sqlite3.connect("quakes")
    cursor = conn.cursor()
    totalExecutionTime = 0
    columns = ['time', 'latitude', 'longitude', 'place', 'mag']

    # without cache
    if withCache == 0:
        # print("hi!")

        magnitude_value = round(random.uniform(magnitudeStart, magnitudeEnd), 1)
        startTime = time.time()
        query = "SELECT locationSource FROM earthquake_new WHERE mag = '" + str(magnitude_value) + "'"
        cursor.execute(query)
        endTime = time.time()
        # print(query)
        lstDictionaryDataDisplay = cursor.fetchall()
        # print(lstDictionaryDataDisplay)
        executionTime = (endTime - startTime) * 1000
        firstExecutionTime = executionTime

        for i in range(noOfQueries-1):
            totalExecutionTime = totalExecutionTime + executionTime
            magnitude_value = round(random.uniform(magnitudeStart, magnitudeEnd), 1)
            startTime = time.time()
            query = "SELECT locationSource FROM earthquake_new WHERE mag = '" + str(magnitude_value) + "'"
            cursor.execute(query)
            endTime = time.time()
            lstDictionaryData = list(cursor.fetchall())
            # print("inside if")
            # print(lstDictionaryData)

            memData = []
            for row in lstDictionaryData:
                memDataDict = dict()
                for i, val in enumerate(row):
                    if type(val) == datetime:
                        val = time.mktime(val.timetuple())
                    memDataDict[columns[i]] = val
                memData.append(memDataDict)
            r.set(query, dumps(memData))

            executionTime = (endTime - startTime) * 1000
            # totalExecutionTime = totalExecutionTime + executionTime
        # print(totalExecutionTime)
    # with cache
    else:
        for i in range(noOfQueries):
            magnitude_value = round(random.uniform(1, 10), 2)
            query = "SELECT locationSource FROM earthquake_new WHERE mag = '" + str(magnitude_value) + "'"
            # print("inside else")
            memhash = hashlib.sha256(query.encode()).hexdigest()
            startTime = time.time()
            lstDictionaryData = r.get(memhash)

            # print(lstDictionaryData[0])
            # print(i)
            if not lstDictionaryData:
                # print("from db")

                cursor.execute(query)
                lstDictionaryData = cursor.fetchall()
                if i == 0:
                    # print("from db")
                    lstDictionaryDataDisplay = lstDictionaryData
                endTime = time.time()
                memData = []
                for row in lstDictionaryData:
                    memDataDict = dict()
                    for i, val in enumerate(row):
                        if type(val) == datetime:
                            val = time.mktime(val.timetuple())
                        memDataDict[columns[i]] = val
                    memData.append(memDataDict)
                r.set(memhash, dumps(memData))
            else:
                lstDictionaryData = loads(lstDictionaryData.decode())
                if i == 0:
                    lstDictionaryDataDisplay = lstDictionaryData
                endTime = time.time()
            executionTime = (endTime - startTime) * 1000
            if i == 0:
                firstExecutionTime = executionTime
            totalExecutionTime = totalExecutionTime + executionTime
    conn.close()
    # print(lstDictionaryData)
    return render_template('index.html', tableData=lstDictionaryDataDisplay, tableDataLen=lstDictionaryDataDisplay._len_(), executionTime=totalExecutionTime, firstExecutionTime=firstExecutionTime)


# @app.route('/upload_  data', methods=['POST'])
# def upload():
#     sqlquery = 'Insert into "all_month" ({columns}) values ({values})'
#     tempfile = request.files['files']
#     filename = 'tempcsv'
#     tempfile.save(os.path.join(filename))
#
#     csv_file = open(filename, 'r')
#     reader = csv.DictReader(csv_file)
#     for row in reader:
#         cols = '"' + '","'.join(row.keys()) + '"'
#         vals = '\'' + '\',\''.join(row.values()) + '\''
#         q = sqlquery.format(columns=cols, values=vals)
#         print("QUERy:", q)
#         cursor = database.connection.cursor()
#         cursor.execute(q)
#     csv_file.close()

# @app.route('/uploaddata')
# def uploadData():
#     return render_template('index.html', tableData=lstDictionaryData, tableDataLen=lstDictionaryData._len_())

if _name_ == '_main_':
  app.run(port = int(port), debug=True)


# karthik code


@app.route('/randomqueries',methods=['GET','POST'])
def randomqueries():
    con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:karthikgunalan.database.windows.net,1433;Database=assignment3;Uid=karthikgunalan@karthikgunalan;Pwd={Polo5590};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)
    minmag = int(request.form['minmag'])
    maxmag = int(request.form['maxmag'])
    count = int(request.form['count'])
    a=count
    countwithincache=0
    countwithindb=0
    columns = ['time', 'latitude', 'longitude','mag']
    execution_of_time_in_db=[]
    execution_of_time_in_cache=[]

    val=float(minmag)
    interval=[]
    interval.append(minmag)
    
    while val<maxmag:
        val+=0.1
        interval.append(round(val,2))
    
    for i in range(0,len(interval)-1):
        cur = con.cursor()
        # index=round((random.uniform(0,len(interval)-2)))
        while count>0:
            
            index=round((random.uniform(0,len(interval)-2)))
            query="select time,latitude,longitude,mag from all_month where mag > "+str(interval[index])
            start=time.time()
            result = r.get(query)
            
            
            if result is None:
                print('in db')
                
                cur=con.cursor()
                cur.execute(query)
                rows=list(cur.fetchall())
                end=time.time()
                execution_of_time_in_db.append(end-start)
                mem=[]

                for row in rows:
                    memdict=dict()
                    for j,val in enumerate(row):
                        memdict[columns[j]]=val
                    mem.append(memdict)
                r.set(query,dumps(mem))
                
                countwithindb=countwithindb+1
            else:
                result=loads(result.decode("utf-8"))
                end=time.time()
                execution_of_time_in_cache.append(end-start)
                resultdisplay=result
                print('in cache')
                
                countwithincache=countwithincache+1
            count=count-1
                    
    probability_of_occurence_in_db=countwithindb/a
    probability_of_occurence_in_cache=countwithincache/a
    # print("probability of query hitting db=",probability_of_occurence_in_db)
    # print("probability of query hitting cache=",probability_of_occurence_in_cache)
        # return render_template('magnitude2.html',rows=result,time=endmem-startmem)
    sum_dbtime=sum(execution_of_time_in_db)
    sum_cachetime=sum(execution_of_time_in_cache)
    con.close()
    return render_template('randomqueries.html',probdb=probability_of_occurence_in_db,probcache=probability_of_occurence_in_cache,timedb=sum_dbtime,timecache=sum_cachetime)




