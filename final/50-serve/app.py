import ujson as json
import mysql.connector
import pyhive
import py2neo
from flask import Flask, request
from flask_cors import CORS

cnx = mysql.connector.connect(
    **{
        "host": "localhost",
        "database": "warehouse",
        "username": "root",
        "password": "root"
    }
)
cursor = cnx.cursor()

# neo = py2neo.Connection(
#     {
#         "host": "localhost",
#         "database": "warehouse",
#         "username": "warehouse",
#         "password": "warehouse",
#     }
# )

app = Flask(__name__)
CORS(app)

'''
按照时间进行查询及统计（例如XX年有多少电影，XX年XX月有多少电影，XX年XX季度有多少电影，周二新增多少电影等）
按照电影名称进行查询及统计（例如 XX电影共有多少版本等）
按照导演进行查询及统计（例如 XX导演共有多少电影等）
按照演员进行查询及统计（例如 XX演员主演多少电影，XX演员参演多少电影等）
按照演员和导演的关系进行查询及统计（例如经常合作的演员有哪些，经常合作的导演和演员有哪些）
按照电影类别进行查询及统计（例如 Action电影共有多少，Adventure电影共有多少等）
按照用户评价进行查询及统计（例如用户评分3分以上的电影有哪些，用户评价中有正面评价的电影有哪些等）
按照上述条件的组合查询和统计
'''

@app.route("/api/mysql/")
def getProductByTitleLike():
    keyword = request.args.get("keyword")
    cursor.execute("select * from product where title like %s", (f"%{keyword}%",))
    res = cursor.fetchall()
    return json.dumps(res)



if __name__ == "__main__":
    app.run("localhost", "8080", True)

