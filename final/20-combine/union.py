import tqdm
import ujson as json
import UnionFind

data = []
pid_list = set()


with open("data/extract/movie.jl") as movie:
    for line in movie:
        j = json.loads(line)
        data.append(j)
        pid_list.append(j["pid"])

myUnionFind = UnionFind.UnionFind(pid_list)

for line_text in tqdm.tqdm(data):
    self_pid = line_text["pid"]
    for asin in line_text["otherFormat"]:
        if asin in pid_list:
            myUnionFind.union(self_pid, asin)
    if "additionalOptions" in line_text:
        for asin in line_text["additionalOptions"]:
            if asin in pid_list:
                myUnionFind.union(self_pid, asin)

print("Final Components: ", myUnionFind.n_comps)
print("Final Elements: ", myUnionFind.n_elts)

with open("components.txt", "w", encoding="utf-8") as outputfile:
    print(myUnionFind.components(), file=outputfile)

with open("component_mapping.txt", "w", encoding="utf-8") as outputfile:
    print(myUnionFind.component_mapping(), file=outputfile)
