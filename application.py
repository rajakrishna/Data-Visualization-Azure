from flask import Flask,render_template,request
import os
import pyodbc
app = Flask(__name__)

@app.route('/')
def home():
   return render_template("home.html")


   
@app.route('/magnitude', methods=['GET', 'POST'])
def magnitude():
    # server = 'hello1997.database.windows.net'
    # database = 'quakes'
    # username = 'raja'
    # password = 'Frankster@12'
    # driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("SELECT count(*) from all_month")
    row = cursor.fetchall()
    return render_template("magnitude.html", row=row)

@app.route('/location', methods=['GET', 'POST'])
def location():
    lat1 = request.form['lat1']
    lon1 = request.form['lon1']
    lat2 = request.form['lat2']
    lon2 = request.form['lon2']
    cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = cnxn.cursor()
    cursor.execute("Select * from all_month where longitude >= '"+lon1+"' and longitude <= '"+lon2+"' and latitude >='"+lat1+"' and latitude <= '"+lat2+"' ")
    result = cursor.fetchall()
    print(result)
    return render_template("location.html", row=result)


port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
#    app.run(debug="true",port=port)
     app.run("0.0.0.0",port=port)



