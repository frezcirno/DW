import mysql.connector
import pyhive
import py2neo
from flask import Flask, request
form flask_cors import CORS

cnx = mysql.connector.connect(
    {
        "host": "localhost",
        "database": "warehouse",
        "username": "warehouse",
        "password": "warehouse",
    }
)
cursor = cnx.cursor()

neo = py2neo.Connection(
    {
        "host": "localhost",
        "database": "warehouse",
        "username": "warehouse",
        "password": "warehouse",
    }
)

app = Flask(__name__)
CORS(app)

@app.route("/api/searchByTitle")
def searchByTitle():
    keyword = request.args.get("keyword")
    cursor.execute("select * from product where title like %s", (f"%{keyword}%",))
    return cursor.fetchAll()

