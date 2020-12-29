import re
import tqdm
from datetime import datetime
import ujson as json

raw_products = {}
with open("data/extract/movie.jl") as movie:
    for line in tqdm.tqdm(movie, desc="loading"):
        j = json.loads(line)
        raw_products[j["pid"]] = j

all_movies = set()
parent = {}
with open("data/extract/components.txt") as components:
    for group in map(
        lambda group: list(map(lambda item: item.strip(), group.split(","))),
        components.read()
        .replace("[", "")
        .replace("]", "")
        .replace("{", "")
        .replace("},", "}")
        .replace("'", "")
        .split("}"),
    ):
        if group[0] == "":
            continue
        all_movies.add(group[0])
        for item in group:
            parent[item] = group[0]


monthNames = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


def handleTime(time):
    mts = re.findall("(\w+) (\d+), (\d+)", time)
    y = 0
    m = 0
    d = 0
    if len(mts) > 0:
        mts = mts[0]
        y = int(mts[2])
        if mts[0].isalpha():
            m = monthNames[mts[0]]
        else:
            optMonth = re.findall(".*?(\d+).*?", mts[0])
            if len(optMonth) > 0:
                m = int(optMonth[0])

        d = int(mts[1])
    return f"{y}-{m}-{d}"


all_genres = set()


def handleGenres(genres):
    res = []
    for g in genres.split(","):
        gg = g.strip()
        if gg == "":
            continue
        all_genres.add(gg)
        res.append(gg)
    return res


all_actors = set()


def handleActors(actors):
    res = []
    for a in actors.split(","):
        aa = a.strip()
        if aa == "":
            continue
        all_actors.add(aa)
        res.append(aa)
    return res


all_directors = set()


def handleDirectors(director):
    res = []
    for a in director.replace("and", ",").split(","):
        aa = a.strip()
        if aa == "":
            continue
        all_directors.add(aa)
        res.append(aa)
    return res


with open("cooked.jl", "w", encoding="utf-8") as cooked:
    for pid in tqdm.tqdm(raw_products, desc="cooking"):
        cooked_products = {}
        raw_product = raw_products[pid]
        cooked_products["pid"] = pid
        cooked_products["title"] = raw_product["title"]
        cooked_products["release"] = handleTime(
            raw_product.get("Release date", raw_product.get("Date First Available", ""))
        )
        cooked_products["director"] = handleDirectors(
            raw_product.get("Director", raw_product.get("Directors", ""))
        )
        cooked_products["actors"] = handleActors(
            raw_product.get("Starring", raw_product.get("Actors", ""))
        )
        # TODO: Keyword里面可能有题材关键词
        cooked_products["genres"] = handleGenres(raw_product.get("Genres", ""))
        cooked_products["movie"] = parent[pid]
        cooked_products["star"] = raw_product.get("star", 0)
        cooked_products["pos_review_count"] = 0
        cooked_products["neg_review_count"] = 0
        cooked.write(json.dumps(cooked_products) + "\n")

with open("actor.jl", "w", encoding="utf-8") as actor:
    for actor_name in all_actors:
        actor.write(actor_name + "\n")

with open("director.jl", "w", encoding="utf-8") as director:
    for director_name in all_directors:
        director.write(director_name + "\n")

with open("movie.jl", "w", encoding="utf-8") as movie:
    for movie_name in all_movies:
        movie.write(movie_name + "\n")

with open("genres.jl", "w", encoding="utf-8") as genres:
    for genres_name in all_genres:
        genres.write(genres_name + "\n")
