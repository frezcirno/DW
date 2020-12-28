import re
import ujson as json

dropped = 0
left = 0


def parseTime(time):
    result = 0
    matchs = re.findall("(?:(\d+) hours?)?(?: and )?(?:(\d+) minutes?)?", time)
    if matchs[0][0]:
        result += int(matchs[0][0]) * 60
    if matchs[0][1]:
        result += int(matchs[0][1])
    return result


with open("data/extract/total.jl", encoding="utf-8") as f, open(
    "data/extract/movie.jl", "w", encoding="utf-8"
) as fo:
    for line in f:
        jl = json.loads(line)
        # print(json.dumps(jl, indent=2))

        def no():
            # print("No!")
            global dropped
            dropped += 1

        def yes():
            # print("Yes!")
            fo.write(line)
            global left
            left += 1

        if False:
            pass
        elif "ISBN-10" in jl or "ISBN-13" in jl:
            # 有ISBN说明是书/教材附带的video
            no()
        elif "Instructional" in jl["keywords"]:
            # 教程类的视频
            no()
        elif "MPAA rating" in jl and "s_medNotRated" not in jl["MPAA rating"]:
            # 有分级一定是电影
            yes()
        elif "Directors" in jl or "Director" in jl:
            # 有导演一定是电影
            yes()
        elif "Starring" in jl:
            # 有主演一定是电影
            yes()
        elif "Run time" in jl or "runtime" in jl:
            # 有时长那么很可能是电影
            time = jl.get("Run time", jl.get("runtime"))
            time = parseTime(time)
            if 30 < time < 250:
                yes()
            else:
                no()
        else:
            no()


print(f"Left video: {left}")
print(f"Drop video: {dropped}")
