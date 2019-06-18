from flask import Flask,render_template,request
import os
import pyodbc
import random
import redis
import hashlib
from json import loads, dumps
import time
app = Flask(__name__)

@app.route('/')
def home():
   return render_template("home.html")

myHostname = "raja.redis.cache.windows.net"
myPassword="Qjd4+MYTjcKhRRGyoeX7+i8Tc5gKgUPAXrEi86PzJJs="


@app.route('/magnitude', methods=['GET','POST'])
def mag():
    columns = ['time', 'latitude', 'longitude','mag']
    mag = str(request.form['mag'])
    r = redis.StrictRedis(host=myHostname, port=6380,password=myPassword,ssl=True)
    result = r.get(mag) 
    
    # print(result)  
    if result is None:
        start=time.time()
        print("retrieved from database")
        con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
        cur = con.cursor()
        query="select * from all_month where mag > "+mag
        memhash = hashlib.sha256(query.encode()).hexdigest()
        cur.execute(query)
        rows= list(cur.fetchall())
        mem=[]
        memdata=[]
        
        for row in rows:
            memdict=dict()
            mem=[(0,row[0]),(1,row[1]),(2,row[2]),(3,row[4])]
            # print(mem)
            for j,val in mem:
                memdict[columns[j]]=val
            memdata.append(memdict)
        r.set(mag,dumps(memdata))
        end=time.time()
        print('Time Taken')
        print(end-start)
        return render_template('magnitude1.html',rows=rows)
    else:
        result=loads(result.decode("utf-8"))
        resultdisplay=result
        
        print("retrieved from memcache",result)
        return render_template('magnitude2.html',rows=result)
























# @app.route('/showdb', methods=['GET', 'POST'])
# def showdb():
#     limit = request.form['limit']
#     cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
#     cursor = cnxn.cursor()
#     cursor.execute("SELECT TOP "+limit+" * from all_month ")
#     row = cursor.fetchall()
#     return render_template("showdb.html", row=row)


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
#    app.run(debug="true",port=port)
     app.run("0.0.0.0",port=port)



