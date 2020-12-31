from logging import debug
import tqdm
import ujson as json
import mysql.connector
from multiprocessing import Process, Pool, Manager, Queue

dbconfig = {
    "host": "localhost",
    "database": "warehouse",
    "user": "root",
    "password": "root",
}

cnx = mysql.connector.connect(**dbconfig)
cursor = cnx.cursor()


all_data = []
with open("data/cooked/cooked.jl", encoding="utf-8") as cooked:
    for line in tqdm.tqdm(cooked, desc="cooked"):
        all_data.append(json.loads(line))

params = []
opnd = ""
count = 0
with open("time.txt", "w") as time:
    for data in tqdm.tqdm(all_data, desc="ymd"):
        pid = data["pid"]
        y, m, d = tuple(map(int, data["release"].split("-")))
        params.append((pid, y, m, d))
        # time.write(f"{pid},{y},{m},{d}"))
        opnd += f"('{pid}',{y},{m},{d}),"
        count += 1
        if count == 10000:
            cursor.execute("insert into new_table (asin,y,m,d) values " + opnd[:-1])
            opnd = ""
            count = 0
    if count:
        cursor.execute("insert into new_table values " + opnd[:-1])
        opnd = ""
        count = 0

cnx.commit()

cursor.close()
cnx.close()
