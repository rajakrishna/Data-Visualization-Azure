from flask import Flask
import os
app = Flask(__name__)

@app.route("/")
def hello():
    return "Hello World!"

port = int(os.getenv("PORT", 5000))
if __name__ == '__main__':
  app.run(debug="true",port=port)
#    app.run("0.0.0.0",port=port)
