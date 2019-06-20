"""Cloud Foundry test"""
from flask import Flask,render_template
import os
import pyodbc
import csv

app = Flask(__name__)

port = int(os.getenv("PORT", 5000))

@app.route('/')
def hello_world():
    con = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};Server=tcp:hello1997.database.windows.net,1433;Database=quakes;Uid=raja@hello1997;Pwd={azure@123};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    query="Select mag,latitude from quake3 where mag between 7 and 8"
    columns=['mag','latitude']
    dic=dict()
    cur=con.cursor()
    mem=[]
    cur.execute(query)
    result=list(cur.fetchall())
    for row in result:
        memdict=dict()
        for j,val in enumerate(row):
            memdict[columns[j]]=val
        mem.append(memdict)
    # a=[1,2,3,4,5]
    return render_template('chart.html',a=mem,chart="pie")



if __name__ == '__main__':
    app.run(port=port,debug=True)
