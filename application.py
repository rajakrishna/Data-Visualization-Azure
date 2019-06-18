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

# @app.route('/createtable', methods=['GET','POST'])
# def createtable():
#     con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#     cursor = con.cursor()
#     start_time = time.time()
#     cursor.execute("CREATE TABLE quakes.dbo.friend2(\
#             [state] [nvarchar](20) NULL,\
#             [2010] [nvarchar](10) NULL,\
#             [2011] [nvarchar](10) NULL,\
#             [2012] [nvarchar](10) NULL,\
#             [2013] [nvarchar](10) NULL,\
#             [2014] [nvarchar](10) NULL,\
#             [2015] [nvarchar](10) NULL,\
#             [2016] [nvarchar](10) NULL,\
#             [2017] [nvarchar](10) NULL,\
#             [2018] [nvarchar](10) NULL)")
#     con.commit()
#     end_time = time.time()
#     con.close()
#     time_taken = (end_time - start_time) * 1000
#     return time_taken






# @app.route('/magnitude', methods=['GET','POST'])
# def mag():
#     columns = ['time', 'latitude', 'longitude','mag']
#     mag = str(request.form['mag'])
#     r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)
#     result = r.get(mag) 
    
#     # print(result)  
#     if result is None:
#         start=time.time()
#         print("retrieved from database")
#         con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#         cur = con.cursor()
#         query="select * from all_month where mag > "+mag
#         memhash = hashlib.sha256(query.encode()).hexdigest()
#         cur.execute(query)
#         rows= list(cur.fetchall())
#         mem=[]
#         memdata=[]
        
#         for row in rows:
#             memdict=dict()
#             mem=[(0,row[0]),(1,row[1]),(2,row[2]),(3,row[4])]
#             # print(mem)
#             for j,val in mem:
#                 memdict[columns[j]]=val
#             memdata.append(memdict)
#         r.set(mag,dumps(memdata))
#         end=time.time()
#         print('Time Taken')
#         print(end-start)
#         return render_template('magnitude1.html',rows=rows)
#     else:
#         result=loads(result.decode("utf-8"))
#         resultdisplay=result
        
#         print("retrieved from memcache",result)
#         return render_template('magnitude2.html',rows=result)



@app.route('/randomqueries', methods=['GET', 'POST'])
def randomQueries():
    magstart = int(request.form['minmag'])
    magend = int(request.form['maxmag'])
    noOfQueries = int(request.form['count'])
    withCache = int(request.form['Cache'])
    list_data = []
    list_total_dict = []

    conn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = conn.cursor()
    totalExecutionTime = 0
    columns = ['time', 'latitude', 'longitude', 'place', 'mag']

    if withCache == 0:
        magval = round(random.uniform(magstart, magend), 1)
        print(magval)
        startTime = time.time()
        query = "SELECT 'time', latitude, longitude, place, mag FROM all_month WHERE mag = '" + str(magval) + "'"
        cursor.execute(query)
        endTime = time.time()
        list_total_dict = cursor.fetchall()
        executionTime = (endTime - startTime) * 1000
        firstExecutionTime = executionTime

        for i in range(noOfQueries-1):
            totalExecutionTime = totalExecutionTime + executionTime
            magval = round(random.uniform(magstart, magend), 1)
            startTime = time.time()
            query = "SELECT 'time', latitude , longitude, place, mag FROM all_month WHERE mag = '" + str(magval) + "'"
            cursor.execute(query)
            endTime = time.time()
            list_data = list(cursor.fetchall())
          
            memData = []
            for row in list_data:
                memDataDict = dict()
                for i, val in enumerate(row):
                  
                    memDataDict[columns[i]] = val
                memData.append(memDataDict)
            r.set(query, dumps(memData))

            executionTime = (endTime - startTime) * 1000
         
    else:
        print('It is inside Redis now')
        for x in range(noOfQueries):
            print('x')
            print(x)
            magval = round(random.uniform(magstart, magend), 1)
            query = "SELECT 'time', latitude , longitude, place, mag FROM all_month WHERE mag = '" + str(magval) + "'"           
            memhash = hashlib.sha256(query.encode()).hexdigest()
            startTime = time.time()
            list_data = r.get(memhash)

            if not list_data:             
                print('Not in cache')
                cursor.execute(query)
                list_data = cursor.fetchall()             
                if x == 0:
                    list_total_dict = list_data
                endTime = time.time()
                memData = []
                for row in list_data:
                    memDataDict = dict()
                    for i, val in enumerate(row):
                        memDataDict[columns[i]] = val
                    memData.append(memDataDict)
                r.set(memhash, dumps(memData))
                executionTime = (endTime - startTime) * 1000
                if x == 0:
                    firstExecutionTime = executionTime
                totalExecutionTime = totalExecutionTime + executionTime
                

            else:
                print('In cache')
                list_data = loads(list_data.decode())
                if x == 0:
                    list_total_dict = list_data
                endTime = time.time()
            executionTime = (endTime - startTime) * 1000
            if x == 0:
                    firstExecutionTime = executionTime
            totalExecutionTime = totalExecutionTime + executionTime
    conn.close()
    return render_template('home.html', tableData=list_total_dict, tableDataLen=list_total_dict.__len__(), executionTime=totalExecutionTime, firstExecutionTime=firstExecutionTime)






















@app.route('/showdb', methods=['GET', 'POST'])
def showdb():
    limit = request.form['limit']
    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("SELECT TOP "+limit+" * from all_month ")
    row = cursor.fetchall()
    return render_template("showdb.html", row=row)


# @app.route('/magnitude', methods=['GET', 'POST'])
# def magnitude():
#     cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#     cursor = cnxn.cursor()
#     cursor.execute("SELECT count(*) from all_month")
#     row = cursor.fetchall()
#     return render_template("magnitude.html", row=row)

# @app.route('/location', methods=['GET', 'POST'])
# def location():
#     lat1 = request.form['lat1']
#     lon1 = request.form['lon1']
#     lat2 = request.form['lat2']
#     lon2 = request.form['lon2']
#     cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#     cursor = cnxn.cursor()
#     cursor.execute("Select * from all_month where longitude >= '"+lon1+"' and longitude <= '"+lon2+"' and latitude >='"+lat1+"' and latitude <= '"+lat2+"' ")
#     result = cursor.fetchall()
#     print(result)
#     return render_template("location.html", row=result)



# @app.route('/magsearch',methods=['GET', 'POST'])

# def magsearch():
#         limit = request.form['limit']
#         mag1 = request.form['magnitude1']
#         mag2 = request.form['magnitude2']
#         # mag = random.randrange(mag1,mag2,1)
#         cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#         cursor = cnxn.cursor()
#         for x in random.randrange(mag1,mag2,1)
#         cursor.execute("Select TOP "+limit+" * from all_month WHERE mag= '"+mag+"'")
#         result = cursor.fetchall()
#             print(result)
#         return render_template("magsearch.html", row=result)











port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
    app.run(debug="true",port=port)
    #  app.run("0.0.0.0",port=port)



