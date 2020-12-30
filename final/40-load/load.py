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

with open("actor.jl", encoding="utf-8") as actors:
    for actor in tqdm.tqdm(actors, desc="actors"):
        actor = actor.strip()
        if actor:
            cursor.execute("insert into person (name) values (%s)", (actor,))
with open("director.jl", encoding="utf-8") as directors:
    for director in tqdm.tqdm(directors, desc="directors"):
        director = director.strip()
        if director:
            cursor.execute("insert into person (name) values (%s)", (director,))
cnx.commit()


with open("movie.jl", encoding="utf-8") as movies:
    for movie in tqdm.tqdm(movies, desc="movies"):
        movie = movie.strip()
        if movie:
            cursor.execute("insert into movie (first_asin) values (%s)", (movie,))
cnx.commit()

cursor.execute("select * from person")
persons = dict([(name, pid) for pid, name in cursor.fetchall()])

cursor.execute("select * from movie")
movies = dict([(first_asin, mid) for mid, _title, first_asin in cursor.fetchall()])


all_data = []
with open("cooked.jl", encoding="utf-8") as cooked:
    for line in tqdm.tqdm(cooked, desc="cooked"):
        all_data.append(json.loads(line))

for data in tqdm.tqdm(all_data, desc="product"):
    cursor.execute(
        "insert into product values (%s, %s, %s)",
        (data["pid"], data["title"], movies[data["movie"]]),
    )
cnx.commit()

for data in tqdm.tqdm(all_data, desc="direct"):
    for director in data["directors"]:
        cursor.execute(
            "insert into direct values (%s, %s)", (data["pid"], persons[director]),
        )
cnx.commit()

for data in tqdm.tqdm(all_data, desc="actin"):
    for actor in data["actors"]:
        cursor.execute(
            "insert into actin values (%s, %s)", (data["pid"], persons[actor]),
        )
cnx.commit()

for data in tqdm.tqdm(all_data, desc="product_genres"):
    for genre in data["genres"]:
        cursor.execute(
            "insert into product_genres values (%s, %s)", (data["pid"], genre),
        )
cnx.commit()

cursor.close()
cnx.close()

