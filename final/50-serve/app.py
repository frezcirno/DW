from time import perf_counter
from typing import List
import ujson as json
import mysql.connector
from pyhive import hive
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
cursor = cnx.cursor(buffered=True)

hconn = hive.Connection(host='localhost', port=10000)
hcursor = hconn.cursor()

driver = GraphDatabase.driver("neo4j://katty.top:7687", auth=("neo4j", "warehouse"))
session = driver.session()

app = Flask(__name__)
CORS(app)


def mysql_query(sql, param=None):
    try:
        cursor.execute(sql, param)
    except mysql.connector.errors.OperationalError:
        cnx.reconnect()
    finally:
        cursor.execute(sql, param)

    fields = list(map(lambda x: x[0], cursor.description))
    return [dict(zip(fields, row)) for row in cursor.fetchall()]


def neo4j_query(cypher, param=None):
    def query(tx):
        res = []
        for result in tx.run(cypher, param):
            res.append(result.values())
        return res

    return session.read_transaction(query)


def hive_query(sql):
    global hconn
    try:
        hcursor.execute(sql)
    except Exception:
        hconn = hive.Connection(host='localhost', port=10000)
    finally:
        hcursor.execute(sql)

    fields = list(map(lambda x: x[0], hcursor.description))
    return [dict(zip(fields, row)) for row in hcursor.fetchall()]


"""
按照上述条件的组合查询和统计
"""

season2months = {"1": [1, 2, 3], "2": [4, 5, 6], "3": [7, 8, 9], "4": [10, 11, 12]}


@app.route('/api/combine/movie')
def combine_movie():
    mysql = mysql_movie()
    neo4j = 0  # neo4j_movie()
    hive = hive_movie()

    return {
        'data': mysql['data'],
        'count': mysql['count'],
        'mysql': mysql['time'],
        'neo4j': neo4j['time'],
        'hive': hive['time']
    }


@app.route('/api/combine/any')
def combine_any():
    option = request.args.get("option", "mysql")

    res = []
    if option == 'mysql':
        res = mysql_any()
    elif option == 'neo4j':
        res = neo4j_any()
    elif option == 'hive':
        res = hive_any()

    return {'option': option, 'data': res}


@app.route('/api/combine/product')
def combine_product():
    mysql = mysql_product()
    neo4j = neo4j_product()
    hive = hive_product()

    return {
        'data': mysql['data'],
        'count': mysql['count'],
        'mysql': mysql['time'],
        'neo4j': neo4j['time'],
        'hive': hive['time']
    }


@app.route("/api/mysql/any")
def mysql_any():
    sql = request.args.get("sql", "")
    return mysql_query(sql)


@app.route("/api/mysql/product")
def mysql_product():
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
        where.append("y=%s")
        param.append(y)

    m = request.args.get("m", 0)
    if m:
        _from.add("product")
        where.append("m=%s")
        param.append(m)

    d = request.args.get("d", 0)
    if d:
        _from.add("product")
        where.append("d=%s")
        param.append(d)

    season = request.args.get("season", 0)
    if season:
        _from.add("product")
        where.append("(m=%s or m=%s or m=%s)")
        param += season2months[season]

    weekday = request.args.get("weekday", 0)
    if weekday:
        _from.add("product")
        where.append("weekday=%s")
        param.append(weekday)

    asin = request.args.get("asin", 0)
    if asin:
        _from.add("product")
        where.append("asin=%s")
        param.append(asin)

    title = request.args.get("title", 0)
    if title:
        _from.add("product")
        where.append(f"(title like '%{title}%')")

    rating = request.args.get("rating", 0)
    if rating:
        _from.add("product")
        where.append(f"rating >= {rating}")

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

    genres = request.args.getlist("genre[]")
    if genres:
        _from.add("product")
        _from.add("product_genres")
        where.append(' or '.join(["genre = %s" for _ in genres]))
        param += genres

    # genre = request.args.get("genre", 0)
    # if genre:
    #     where.append("genre = %s")
    #     param.append(genre)

    offset = request.args.get("offset", 0)

    sql = "select *" + " from " + " natural join ".join(_from) + \
          " where " + " and ".join(where) + f" limit {offset},100"

    count_sql = (
        "select count(1) as num" + " from " + " natural join ".join(_from) + " where " + " and ".join(where)
    )

    start = perf_counter()
    res = mysql_query(sql, param)
    time = 1000 * (perf_counter() - start)

    count = mysql_query(count_sql, param)

    return {"count": count[0]["num"], 'time': time, "data": res}


@app.route('/api/mysql/movie')
def mysql_movie():
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
        where.append("y=%s")
        param.append(y)

    m = request.args.get("m", 0)
    if m:
        _from.add("product")
        where.append("m=%s")
        param.append(m)

    d = request.args.get("d", 0)
    if d:
        _from.add("product")
        where.append("d=%s")
        param.append(d)

    season = request.args.get("season", 0)
    if season:
        _from.add("product")
        where.append("(m=%s or m=%s or m=%s)")
        param += season2months[season]

    weekday = request.args.get("weekday", 0)
    if weekday:
        _from.add("product")
        where.append("weekday=%s")
        param.append(weekday)

    asin = request.args.get("asin", 0)
    if asin:
        _from.add("product")
        where.append("asin=%s")
        param.append(asin)

    title = request.args.get("title", 0)
    if title:
        _from.add("product")
        where.append(f"(title like '%{title}%')")

    rating = request.args.get("rating", 0)
    if rating:
        _from.add("product")
        where.append(f"rating >= {rating}")

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

    genres = request.args.getlist("genre[]")
    if genres:
        _from.add("product")
        _from.add("product_genres")
        where.append(' or '.join(["genre = %s" for _ in genres]))
        param += genres

    # genre = request.args.getlist("genre", 0)
    # if genre:
    #     where.append("genre = %s")
    #     param.append(genre)

    offset = request.args.get("offset", 0)

    sql = "select product.movie" + " from " + " natural join ".join(_from) + \
          " where " + " and ".join(where) + f" limit {offset},100"

    sql = f"select * from product where movie=({sql})"

    try:
        start = perf_counter()
        res = mysql_query(sql, param)
        time = 1000 * (perf_counter() - start)
    except mysql.connector.errors.DataError:
        res = []
        time = 0

    return {"count": len(res), 'time': time, "data": res}


@app.route("/api/hive/any")
def hive_any():
    sql = request.args.get("sql", "")
    return hive_query(sql)


@app.route('/api/hive/product')
def hive_product():
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
    fromStr = "product pr"

    y = request.args.get("y", 0)
    if y:
        where.append(f"y={y}")

    m = request.args.get("m", 0)
    if m:
        where.append(f"m={m}")

    d = request.args.get("d", 0)
    if d:
        where.append(f"d={d}")

    season = request.args.get("season", 0)
    if season:
        where.append(f"(m={season2months[season][0]} or m={season2months[season][1]} or m={season2months[season][2]})")

    weekday = request.args.get("weekday", 0)
    if weekday:
        where.append(f"weekday={weekday}")

    asin = request.args.get("asin", 0)
    if asin:
        where.append(f"asin='{asin}'")

    title = request.args.get("title", 0)
    if title:
        where.append(f"(title like '%{title}%')")

    rating = request.args.get("rating", 0)
    if rating:
        where.append(f"rating >= {rating}")

    director = request.args.get("director", 0)
    if director:
        fromStr += " join product_director pd on pr.asin = pd.asin "
        where.append(f"director = '{director}'")

    actor = request.args.get("actor", 0)
    if actor:
        fromStr += " join product_actor pa on pr.asin = pa.asin "
        where.append(f"actor = '{actor}'")

    support_actor = request.args.get("support_actor", 0)
    if support_actor:
        fromStr += " join product_support_actor psa on pr.asin = psa.asin "
        where.append(f"support_actor = '{support_actor}'")

    genres = request.args.getlist("genre[]")
    if genres:
        fromStr += " join product_genres pg on pr.asin = pg.asin "
        where.append(' or '.join([f"genre = '{g}'" for g in genres]))

    offset = request.args.get("offset", 0)

    sql = "select * from " + fromStr + \
          " where " + " and ".join(where) + f" limit {offset}, 100"

    count_sql = "select count(1) as num from " + fromStr + " where " + " and ".join(where)

    start = perf_counter()
    res = hive_query(sql)
    time = 1000 * (perf_counter() - start)

    count = hive_query(count_sql)

    return {"count": count[0]["num"], 'time': time, "data": res}


@app.route('/api/hive/movie')
def hive_movie():
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
    fromStr = "product pr"

    y = request.args.get("y", 0)
    if y:
        where.append(f"y={y}")

    m = request.args.get("m", 0)
    if m:
        where.append(f"m={m}")

    d = request.args.get("d", 0)
    if d:
        where.append(f"d={d}")

    season = request.args.get("season", 0)
    if season:
        where.append(f"(m={season2months[season][0]} or m={season2months[season][1]} or m={season2months[season][2]})")

    weekday = request.args.get("weekday", 0)
    if weekday:
        where.append(f"weekday={weekday}")

    asin = request.args.get("asin", 0)
    if asin:
        where.append(f"asin='{asin}'")

    title = request.args.get("title", 0)
    if title:
        where.append(f"(title like '%{title}%')")

    rating = request.args.get("rating", 0)
    if rating:
        where.append(f"rating >= {rating}")

    director = request.args.get("director", 0)
    if director:
        fromStr += " join product_director pd on pr.asin = pd.asin "
        where.append(f"director = '{director}'")

    actor = request.args.get("actor", 0)
    if actor:
        fromStr += " join product_actor pa on pr.asin = pa.asin "
        where.append(f"actor = '{actor}'")

    support_actor = request.args.get("support_actor", 0)
    if support_actor:
        fromStr += " join product_support_actor psa on pr.asin = psa.asin "
        where.append(f"support_actor = '{support_actor}'")

    genres = request.args.getlist("genre[]")
    if genres:
        fromStr += " join product_genres pg on pr.asin = pg.asin "
        where.append(' or '.join([f"genre = '{g}'" for g in genres]))

    offset = request.args.get("offset", 0)

    sql = "select pr.movie from " + fromStr + \
          " where " + " and ".join(where) + f" limit {offset}, 2"

    sql = f"select * from product where movie in ({sql})"

    try:
        start = perf_counter()
        res = hive_query(sql)
        time = 1000 * (perf_counter() - start)
        if len(res) > 1:
            raise Exception()
    except Exception:
        res = []
        time = 0

    return {"count": len(res), 'time': time, "data": res}


@app.route("/api/neo4j/relation")
def neo4j_relation():
    """
    按照演员和导演的关系进行查询及统计（例如经常合作的演员有哪些，经常合作的导演和演员有哪些）
    """
    director = request.args.get("director", 0)
    actor = request.args.get("actor", 0)
    support_actor = request.args.get("support_actor", 0)
    skip = request.args.get("skip", 0)

    if director and actor:
        cypher = f'match (a:Actor{{ actor:"{actor}" }})--(p:Product)--(d:Director{{ director:"{director}" }}) RETURN p'
    elif director and support_actor:
        cypher = f'match (s:SupportActor{{ support_actor:"{support_actor}" }})--(p:Product)--(d:Director{{ director:"{director}" }}) RETURN p'
    elif actor and support_actor:
        cypher = f'match (a:Actor{{ actor:"{actor}" }})--(p:Product)--(s:SupportActor{{ support_actor:"{support_actor}" }}) RETURN p'
    else:
        return {"count": 0, 'time': 0, "data": []}

    print(cypher)

    start = perf_counter()
    res = neo4j_query(cypher)
    time = 1000 * (perf_counter() - start)

    res = [dict(zip(row[0].keys(), row[0].values())) for row in res]
    return {"count": len(res), 'time': time, "data": res}


@app.route("/api/neo4j/any")
def neo4j_any():
    cypher = request.args.get("sql", 0)
    return neo4j_query(cypher)


@app.route("/api/neo4j/product")
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
        where.append(f"p.m='{months[0]}' or p.m='{months[1]}' or p.m='{months[2]}'")

    asin = request.args.get("asin", 0)
    if asin:
        where.append(f"p.asin='{asin}'")

    weekday = request.args.get("weekday", 0)
    if weekday:
        where.append(f"p.weekday='{weekday}'")

    title = request.args.get("title", 0)
    if title:
        where.append(f"p.title =~ '.*{title}.*'")

    rating = request.args.get("rating", 0)
    if rating:
        where.append(f"p.rating >= '{rating}'")

    director = request.args.get("director", 0)
    if director:
        where.append(f"p.director >= '{director}'")

    actor = request.args.get("actor", 0)
    if actor:
        where.append(f"a.actor = '{actor}'")

    support_actor = request.args.get("support_actor", 0)
    if support_actor:
        where.append(f"s.support_actor = '{support_actor}'")

    genres = request.args.getlist("genre[]")
    if genres:
        stat = ' or '.join([f"g.genre = '{g}'" for g in genres])
        where.append(f"({stat})")

    # elif genre:
    #     where.append(f"g.genre = '{genre}'")

    skip = request.args.get("skip", 0)

    cypher = f"MATCH (p:Product)-[pd]-(d:Director), (p)-[pa]-(a:Actor), (p)-[ps]-(s:Support_actor), (p)-[pg]-(g:Genre) WHERE {' and '.join(where)} RETURN p skip {skip} limit 20"

    cypher_count = f"MATCH (p:Product)-[pd]-(d:Director), (p)-[pa]-(a:Actor), (p)-[ps]-(s:Support_actor), (p)-[pg]-(g:Genre) WHERE {' and '.join(where)} RETURN count(p)"

    print(cypher)

    start = perf_counter()
    res = neo4j_query(cypher)
    time = 1000 * (perf_counter() - start)

    res_count = neo4j_query(cypher_count)

    res = [dict(zip(row[0].keys(), row[0].values())) for row in res]
    return {"count": res_count[0][0], 'time': time, "data": res}


if __name__ == "__main__":
    app.run("localhost", "8765", True)
