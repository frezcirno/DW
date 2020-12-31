import ujson as json
import mysql.connector
import pyhive
import py2neo
from flask import Flask, request
from flask_cors import CORS

cnx = mysql.connector.connect(
    **{
        "host": "katty.top",
        "database": "warehouse2",
        "username": "warehouse",
        "password": "warehouse",
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

"""
按照时间进行查询及统计（例如XX年有多少电影，XX年XX月有多少电影，XX年XX季度有多少电影，周二新增多少电影等）
按照电影名称进行查询及统计（例如 XX电影共有多少版本等）
按照导演进行查询及统计（例如 XX导演共有多少电影等）
按照演员进行查询及统计（例如 XX演员主演多少电影，XX演员参演多少电影等）
按照演员和导演的关系进行查询及统计（例如经常合作的演员有哪些，经常合作的导演和演员有哪些）
按照电影类别进行查询及统计（例如 Action电影共有多少，Adventure电影共有多少等）
按照用户评价进行查询及统计（例如用户评分3分以上的电影有哪些，用户评价中有正面评价的电影有哪些等）
按照上述条件的组合查询和统计
"""

season2months = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [10, 11, 12]}


@app.route("/api/search")
def search():
    where = []
    param = []

    y = request.args.get("y", 0)
    if y:
        where.append("(y=%s)")
        param.append(y)

    m = request.args.get("m", 0)
    if m:
        where.append("(m=%s)")
        param.append(m)

    d = request.args.get("d", 0)
    if d:
        where.append("(d=%s)")
        param.append(d)

    season = request.args.get("season", 0)
    if season:
        where.append("(m=%s or m=%s or m=%s)")
        param += season2months[season]

    weekday = request.args.get("weekday", 0)
    if weekday:
        where.append("(weekday=%s)")
        param.append(weekday)

    title = request.args.get("title", 0)
    if title:
        where.append("(title like %s)")
        param.append(f"%{title}%")

    sql = "select * from product where " + " and ".join(where)

    print(sql)
    print(param)

    cursor.execute(sql, param)
    res = cursor.fetchall()
    return {"count": len(res), "data": res}


if __name__ == "__main__":
    app.run("localhost", "8080", True)

