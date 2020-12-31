import re
import csv
import tqdm
from datetime import datetime
import ujson as json

raw_products = dict()
with open("data/extract/movie.jl") as movie:
    for line in tqdm.tqdm(movie, desc="loading"):
        j = json.loads(line)
        raw_products[j["pid"]] = j

with open("data/extract/patch_rating.txt") as patch_rating2:
    for line in tqdm.tqdm(patch_rating2, desc="patch_rating"):
        j = json.loads(line)
        pid = j["id"]
        if pid not in raw_products:
            continue
        raw_product = raw_products[pid]
        raw_product["rating_score"] = j.get("Points", "0")
        raw_product["rating_count"] = j.get("PointPersonNumber", "0")
        stars = j.get("Stars", {})
        raw_product["rating5_ratio"] = int(stars.get("5 star", "0%")[:-1]) / 100
        raw_product["rating4_ratio"] = int(stars.get("4 star", "0%")[:-1]) / 100
        raw_product["rating3_ratio"] = int(stars.get("3 star", "0%")[:-1]) / 100
        raw_product["rating2_ratio"] = int(stars.get("2 star", "0%")[:-1]) / 100
        raw_product["rating1_ratio"] = int(stars.get("1 star", "0%")[:-1]) / 100


with open("data/extract/patch_rating2.jl") as patch_rating2:
    for line in tqdm.tqdm(patch_rating2, desc="patch_rating2"):
        j = json.loads(line)
        pid = j["pid"]
        if pid not in raw_products:
            continue
        raw_product = raw_products[pid]
        rating = j.get("star", "0")
        rating = re.findall(".*?(\d+(?:\.\d+)?) out of 5 stars(?: by (\d+).*?)?", rating)[0]
        raw_product["rating_score"] = rating[0] or "0"
        raw_product["rating_count"] = rating[1] or "0"
        raw_product["rating5_ratio"] = 0
        raw_product["rating4_ratio"] = 0
        raw_product["rating3_ratio"] = 0
        raw_product["rating2_ratio"] = 0
        raw_product["rating1_ratio"] = 0


with open("missing_rating.txt", "w") as missing_rating:
    for pid in raw_products:
        raw_product = raw_products[pid]
        if "rating_score" not in raw_product:
            missing_rating.write(pid + "\n")

with open("data/extract/patch_time.txt") as patch_time:
    for line in tqdm.tqdm(patch_time, desc="patch_time"):
        j = json.loads(line)
        pid = j["id"]
        if pid not in raw_products:
            continue
        raw_product = raw_products[pid]
        raw_product["Year"] = j.get("ShowTime", "0")


all_movies = set()
parent = dict()
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
    if time.isdigit():
        return f"{time}-0-0"
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
    res = set()
    for g in genres.replace("and", "").split(","):
        gg = g.strip()
        if gg == "":
            continue
        if gg in res:
            continue
        all_genres.add(gg)
        res.add(gg)
    return list(res)


all_actors = set()


def handleActors(actors):
    res = set()
    aa = ""
    for a in re.split(
        "\r|\n|,|;|/| - |\\\\",
        actors.replace(" and ", ",").replace(" And ", ",").replace(" AND ", ","),
    ):
        aa = a.strip()
        if aa == "":
            continue
        if aa in res:
            continue
        all_actors.add(aa)
        res.add(aa)
    return list(res)


all_directors = set()


def handleDirectors(director):
    res = set()
    for a in re.split(
        "\r|\n|,|;|/| - |\\\\",
        director.replace(" and ", ",").replace(" And ", ",").replace(" AND ", ","),
    ):
        aa = a.strip()
        if aa == "":
            continue
        if aa in res:
            continue
        all_directors.add(aa)
        res.add(aa)
    return list(res)


all_cooked = dict()
with open("cooked.jl", "w", encoding="utf-8") as cooked:
    for pid in tqdm.tqdm(raw_products, desc="cooking"):
        cooked_products = {}
        raw_product = raw_products[pid]
        cooked_products["pid"] = pid
        cooked_products["title"] = raw_product["title"]
        cooked_products["release"] = handleTime(
            raw_product.get(
                "Release date",
                raw_product.get("Date First Available", raw_product.get("Year", "")),
            )
        )
        cooked_products["directors"] = handleDirectors(
            raw_product.get("Director", raw_product.get("Directors", ""))
        )
        cooked_products["actors"] = handleActors(
            raw_product.get("Starring", raw_product.get("Actors", ""))
        )
        # TODO: Keyword里面可能有题材关键词
        cooked_products["genres"] = handleGenres(raw_product.get("Genres", ""))
        cooked_products["movie"] = parent[pid]
        cooked_products["rating"] = raw_product.get("rating_score", 0)
        rating_count = int(raw_product.get("rating_count", "0").replace(",", ""))
        cooked_products["pos_review_count"] = rating_count and int(
            rating_count
            * (
                float(raw_product["rating5_ratio"])
                + float(raw_product["rating4_ratio"])
            )
        )
        cooked_products["neg_review_count"] = rating_count and int(
            rating_count
            * (
                float(raw_product["rating2_ratio"])
                + float(raw_product["rating1_ratio"])
            )
        )
        all_cooked[pid] = cooked_products
        cooked.write(json.dumps(cooked_products) + "\n")


with open("product_actor.csv", "w", encoding="utf-8", newline="") as product_actor:
    product_actor = csv.writer(product_actor)
    product_actor.writerow(["asin", "actor"])
    for pid in tqdm.tqdm(all_cooked, desc="product_actor"):
        data = all_cooked[pid]
        for actor in data["actors"]:
            product_actor.writerow([pid, actor])

with open(
    "product_director.csv", "w", encoding="utf-8", newline=""
) as product_director:
    product_director = csv.writer(product_director)
    product_director.writerow(["asin", "director"])
    for pid in tqdm.tqdm(all_cooked, desc="product_director"):
        data = all_cooked[pid]
        for director in data["directors"]:
            product_director.writerow([pid, director])

with open("movie.csv", "w", encoding="utf-8", newline="") as movie:
    movie = csv.writer(movie)
    movie.writerow(["first_asin"])
    for first_asin in all_movies:
        movie.writerow([first_asin])

with open("product_genres.csv", "w", encoding="utf-8", newline="") as product_genres:
    product_genres = csv.writer(product_genres)
    product_genres.writerow(["asin", "genre"])
    for pid in tqdm.tqdm(all_cooked, desc="product_genres"):
        data = all_cooked[pid]
        for genre in data["genres"]:
            product_genres.writerow([pid, genre])

with open("product.csv", "w", encoding="utf-8", newline="") as product:
    product = csv.writer(product)
    product.writerow(
        ["asin", "title", "movie", "y", "m", "d", "weekday", "rating", "pos", "neg"]
    )
    for pid in tqdm.tqdm(all_cooked, desc="product"):
        data = all_cooked[pid]
        y, m, d = map(int, data["release"].split("-"))
        weekday = datetime(y, m, d).weekday() if y and m and d else 0
        product.writerow(
            [
                pid,
                data["title"],
                data["movie"],
                y,
                m,
                d,
                weekday,
                data["rating"],
                data["pos_review_count"],
                data["neg_review_count"],
            ]
        )

