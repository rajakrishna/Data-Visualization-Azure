from flask import Flask
import os
import pyodbc
app = Flask(__name__)

@app.route("/")
def hello():
   
    server = '<server>.database.windows.net'
    database = '<database>'
    username = '<username>'
    password = '<password>'
    driver= '{ODBC Driver 17 for SQL Server}'
    cnxn = pyodbc.connect('DRIVER='+driver+';SERVER='+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    cursor.execute("SELECT count(*) from all_month")
    row = cursor.fetchone()
    return row