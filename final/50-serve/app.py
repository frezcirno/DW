from typing import AsyncIterable
from neo4j.work import transaction
import ujson as json
import mysql.connector
import pyhive
from neo4j import GraphDatabase
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

driver = GraphDatabase.driver("neo4j://katty.top:7687", auth=("neo4j", "warehouse"))
session = driver.session()

app = Flask(__name__)
CORS(app)

"""
按照演员和导演的关系进行查询及统计（例如经常合作的演员有哪些，经常合作的导演和演员有哪些）
按照上述条件的组合查询和统计
"""

season2months = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [10, 11, 12]}


@app.route("/api/search/mysql")
def mysql_search():
    """
    按照时间进行查询及统计（例如XX年有多少电影，XX年XX月有多少电影，XX年XX季度有多少电影，周二新增多少电影等）
    按照电影名称进行查询及统计（例如 XX电影共有多少版本等）
    按照用户评价进行查询及统计（例如用户评分3分以上的电影有哪些，用户评价中有正面评价的电影有哪些等）
    按照导演进行查询及统计（例如 XX导演共有多少电影等）
    按照演员进行查询及统计（例如 XX演员主演多少电影，XX演员参演多少电影等）
    按照电影类别进行查询及统计（例如 Action电影共有多少，Adventure电影共有多少等）
    """
    _from = set()
    where = []
    param = []

    y = request.args.get("y", 0)
    if y:
        _from.add("product")
        where.append("(y=%s)")
        param.append(y)

    m = request.args.get("m", 0)
    if m:
        _from.add("product")
        where.append("(m=%s)")
        param.append(m)

    d = request.args.get("d", 0)
    if d:
        _from.add("product")
        where.append("(d=%s)")
        param.append(d)

    season = request.args.get("season", 0)
    if season:
        _from.add("product")
        where.append("(m=%s or m=%s or m=%s)")
        param += season2months[season]

    weekday = request.args.get("weekday", 0)
    if weekday:
        _from.add("product")
        where.append("(weekday=%s)")
        param.append(weekday)

    title = request.args.get("title", 0)
    if title:
        _from.add("product")
        where.append("(title like %s)")
        param.append(f"%{title}%")

    rating = request.args.get("rating", 0)
    if rating:
        _from.add("product")
        where.append("rating >= %s")
        param.append(rating)

    director = request.args.get("director", 0)
    if director:
        _from.add("product")
        _from.add("product_director")
        where.append("director = %s")
        param.append(director)

    actor = request.args.get("actor", 0)
    if actor:
        _from.add("product")
        _from.add("product_actor")
        where.append("actor = %s")
        param.append(actor)

    support_actor = request.args.get("support_actor", 0)
    if support_actor:
        _from.add("product")
        _from.add("product_support_actor")
        where.append("support_actor = %s")
        param.append(support_actor)

    genre = request.args.get("genre", 0)
    if genre:
        _from.add("product")
        _from.add("product_genres")
        where.append("genre = %s")
        param.append(genre)

    sql = (
        "select *"
        + " from "
        + " natural join ".join(_from)
        + " where "
        + " and ".join(where)
    )

    print(sql)
    print(param)

    cursor.execute(sql, param)
    res = cursor.fetchall()
    return {"count": len(res), "data": res}


def neo4j_query(cypher, param=None):
    def query(tx):
        res = []
        for result in tx.run(cypher, param):
            res.append(result.values())
        return res
    return session.read_transaction(query)


@app.route("/api/search/neo4j/close")
def neo4j_close():
    count = request.args.get("count", 100)

    query_result = neo4j_query(
        f"MATCH path = (a:Actor)--(p:Product)--(d:Director) WITH a, d, COUNT(*) AS num ORDER BY num DESC RETURN a,d,num limit {count}")

    return {"count": len(query_result)}


@app.route("/api/search/neo4j")
def neo4j_product():
    where = []

    y = request.args.get("y", 0)
    if y:
        where.append(f"p.y='{y}'")

    m = request.args.get("m", 0)
    if m:
        where.append(f"p.m='{m}'")

    d = request.args.get("d", 0)
    if d:
        where.append(f"p.d='{d}'")

    season = request.args.get("season", 0)
    if season:
        months = season2months[season]
        where.append(f"p.m='{months[0]}' or m='{months[1]}' or m='{months[2]}'")

    weekday = request.args.get("weekday", 0)
    if weekday:
        where.append(f"p.weekday='{weekday}'")

    title = request.args.get("title", 0)
    if title:
        where.append(f"p.title =~ '.*{title}.*'")

    rating = request.args.get("rating", 0)
    if rating:
        where.append(f"p.rating >= '{rating}")

    cypher = f"MATCH (p:Product) WHERE {' and '.join(where)} RETURN p"

    print(cypher)

    res = neo4j_query(cypher)

    return {'len': len(res)}


if __name__ == "__main__":
    app.run("localhost", "8080", True)
