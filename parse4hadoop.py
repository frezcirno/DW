import csv

count = 0
lastKeep = 0

with open("movies.txt", encoding="iso-8859-1") as f, open(
    "movies_text.txt", "w", encoding="iso-8859-1"
) as fo:

    for (index, line) in enumerate(f):
        line = line.replace("<br />", "").strip()

        if line == "":
            continue

        elif line.startswith("review/summary:"):
            value = line[16:].strip()
            fo.write(value + "\n")
            count += 1
            lastKeep = 1

        elif line.startswith("review/text:"):
            value = line[13:].strip()
            fo.write(value + "\n")
            count += 1
            lastKeep = 1

        elif line.startswith(("product/", "review/")):
            lastKeep = 0

        else:
            if lastKeep == 1:
                fo.write(line + "\n")
                count += 1

        if index % 100000 == 0:
            print(index)

print("Fin.")
print("Number of lines:", count)

# 多字段
# excel矩阵
# 有很多关联
# 9:00-5:30
# 1-5
