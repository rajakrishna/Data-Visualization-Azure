from flask import Flask,render_template,request
import os
import pyodbc
import random
import redis
import time
import hashlib
from json import loads, dumps



app = Flask(__name__)

myHostname = "raja.redis.cache.windows.net"
myPassword="Qjd4+MYTjcKhRRGyoeX7+i8Tc5gKgUPAXrEi86PzJJs="

r = redis.StrictRedis(host=myHostname, port=6380, password=myPassword, ssl=True)


@app.route('/')
def home():
 return render_template("home.html")



@app.route('/createtable',methods=['GET', 'POST'])
def createTable():
    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = conn.cursor()
    query = 'CREATE TABLE quakes.dbo.all_month2("time" DATETIME,latitude FLOAT,longitude FLOAT,depth FLOAT,mag FLOAT,magType TEXT,nst INT,gap INT,dmin FLOAT,rms FLOAT,net TEXT,id TEXT,updated DATETIME,place TEXT,type TEXT,horontalError FLOAT,depthError FLOAT,magError FLOAT,magNst INT,status TEXT,locationSource TEXT,magSource TEXT)'
    startTime = time.time()
    cursor.execute(query)
    cursor.execute("CREATE INDEX all_month_mag__index ON quakes.dbo.all_month2 (mag)")
    cursor.execute("CREATE INDEX all_month_lat__index ON quakes.dbo.all_month2 (latitude)")
    cursor.execute("CREATE INDEX all_month_long__index ON quakes.dbo.all_month2 (longitude)")
    conn.commit()
    endTime = time.time()
    conn.close()
    executionTime = (endTime - startTime) * 1000
    return render_template('createtable.html', executionTime=executionTime)



@app.route('/randomqueries', methods=['GET', 'POST'])
def randomQueries():
    flat = int(request.form['minmag'])
    slat = int(request.form['maxmag'])
    flatplus = flat+1
    slatminus  = slat - 1

    Num_Queries = int(request.form['count'])
    withCache = int(request.form['Cache'])

    list_dict_Data = []
    list_dict_DataDisplay = []

    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = conn.cursor()
    Tot_Exec_Time = 0
    columns = ['time', 'mag', 'place']

    # without cache
    if withCache == 0:
        exec_time_list = []
        querynum = []
        for i in range(Num_Queries-1):

            
            flat_random = round(random.uniform(flat, flatplus), 1)
            slat_random = round(random.uniform(slatminus, slat), 1)
            Start_Time = time.time()
            query = "Select time,mag,place from quake3 where latitude >='"+str(flat_random)+"' and latitude <= '"+str(slat_random)+"' "
            cursor.execute(query)
            End_Time = time.time()
            list_dict_Data = list(cursor.fetchall())
            list_dict_DataDisplay = cursor.fetchall()
         

            Memory_Data = []
            for row in list_dict_Data:
                Memory_DataDict = dict()
                for i, val in enumerate(row):
                   
                    Memory_DataDict[columns[i]] = val
                Memory_Data.append(Memory_DataDict)
            r.set(query, dumps(Memory_Data))

            Exec_Time = (End_Time - Start_Time) * 1000
            Tot_Exec_Time = Tot_Exec_Time + Exec_Time
            exec_time_list.append(Exec_Time)
            querynum.append(i)

    else:
        print('Cache inside')
        exec_time_list = []
        querynum = []
        for x in range(Num_Queries):

            querynum.append(x)
            print('x')
            print(x)
            flat_random = round(random.uniform(flat, flatplus), 1)
            slat_random = round(random.uniform(slatminus, slat), 1)
            query = "Select time,mag,place from quake3 where latitude >='"+str(flat_random)+"' and latitude <= '"+str(slat_random)+"' "
            # print("inside else")
            Memory_hash = hashlib.sha256(query.encode()).hexdigest()
            Start_Time = time.time()
            list_dict_Data = r.get(Memory_hash)

            if not list_dict_Data:
                # print("from db")
                print('Not in cache')

                cursor.execute(query)
                list_dict_Data = cursor.fetchall()
              
                list_dict_DataDisplay = list_dict_Data
                End_Time = time.time()
                Memory_Data = []
                for row in list_dict_Data:
                  
                    Memory_DataDict = dict()
                    for i, val in enumerate(row):
                     
                        Memory_DataDict[columns[i]] = val
                    Memory_Data.append(Memory_DataDict)
                r.set(Memory_hash, dumps(Memory_Data))
                print('Hi')
                Exec_Time = (End_Time - Start_Time) * 1000
                exec_time_list.append(Exec_Time)


                Tot_Exec_Time = Tot_Exec_Time + Exec_Time


            else:
                print('In cache')
                list_dict_Data = loads(list_dict_Data.decode())
                # if x == 0:
                list_dict_DataDisplay = list_dict_Data
                End_Time = time.time()
                Exec_Time = (End_Time - Start_Time) * 1000
                exec_time_list.append(Exec_Time)
                Tot_Exec_Time = Tot_Exec_Time + Exec_Time
    conn.close()
    # print(list_dict_Data)
    return render_template('home.html', tableData=list_dict_DataDisplay,Exec_Time=Tot_Exec_Time, Each_Exec_Time=exec_time_list, Querynum = querynum)
 



















@app.route('/showdb', methods=['GET', 'POST'])
def showdb():
    limit = request.form['limit']
    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("SELECT TOP "+limit+" * from all_month ")
    row = cursor.fetchall()
    return render_template("showdb.html", row=row)




@app.route('/location', methods=['GET', 'POST'])
def location():
    lat1 = request.form['lat1']

    lat2 = request.form['lat2']

    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("Select time,mag,place from quake3 where latitude >='"+lat1+"' and latitude <= '"+lat2+"' ")
    result = cursor.fetchall()
    print(result)
    return render_template("location.html", row=result)





port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
    app.run(debug="true",port=port)
    #  app.run("0.0.0.0",port=port)



