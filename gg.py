from flask import Flask,render_template,request
import os
import pyodbc
import random
import redis
import hashlib
from json import loads, dumps
import time

con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
cursor = con.cursor()

# start_time = time.time()
# for x in range(100):
#         lon1 = round(random.uniform(0, 100), 10)
#         lat1 = round(random.uniform(0, 3), 10)
#         # lon2 = round(random.uniform(100, 150), 10)
#         # lat2 = round(random.uniform(3, 5), 10)
#         query = "SELECT mag FROM all_month WHERE longitude > '" + str(lon1) + "'and latitude >='" + str(lat1)+"'"
#         # + "'and longitude <= '" + str(lon2) + "'and latitude <= '" + str(lat2) + "'
#         cursor.execute(query)
#         display = cursor.fetchall()
#         con.commit()
#         # print(magval)          
#         # print(lat1,lat2,lon1,lon2)
#         print(lat1,lon1)
#         print(display)
# con.close()
# end_time = time.time()
# time_taken = (end_time - start_time) * 1000



# start_time = time.time()
# for x in range(10):
#         magval = round(random.uniform(1, 8), 1)
#         query = "SELECT id FROM all_month WHERE mag = '" + str(magval) + "'"
#         cursor.execute(query)
#         display = cursor.fetchall()
#         con.commit()
#         print(magval)
#         print(display)
# con.close()
# end_time = time.time()
# time_taken = (end_time - start_time) * 1000
# print(time_taken)


@app.route('/magnitude', methods=['GET','POST'])
def mag():
    columns = ['time', 'latitude', 'longitude','mag']
    mag = str(request.form['mag'])

    r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)
    startmem=time.time()
    query="select time,latitude,longitude,mag from all_month where mag > "+mag
    result = r.get(query) 
    endmem=time.time()
    # print(result)  
    if result is None:
        start=time.time()
        print("retrieved from database")
        con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:karthikgunalan.database.windows.net,1433;Database=assignment3;Uid=karthikgunalan@karthikgunalan;Pwd={Polo5590};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        cur = con.cursor()
        cur.execute(query)
        rows= list(cur.fetchall())
        mem=[]
        for row in rows:
            memdict=dict()
            for j,val in enumerate(row):
                memdict[columns[j]]=val
            mem.append(memdict)
        r.set(query,dumps(mem))
        end=time.time()
        print('Time Taken')
        print(end-start)
        return render_template('magnitude1.html',rows=rows,time=end-start)
    else:
        result=loads(result.decode("utf-8"))
        resultdisplay=result
        print('time taken')
        print(endmem-startmem)  
        return render_template('magnitude2.html',rows=result,time=endmem-startmem)