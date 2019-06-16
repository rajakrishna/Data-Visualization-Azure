from flask import Flask
import os
app = Flask(__name__)

@app.route("/")
def hello():
    hello
     return "Yay yes it is woekring"