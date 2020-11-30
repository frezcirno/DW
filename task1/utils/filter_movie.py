import json

dropped1 = 0
dropped2 = 0

with open('data_fixed.jl', encoding='utf-8') as f, open('movie.jl', 'w', encoding='utf-8') as fo:
    for line in f:
        jl = json.loads(line)
        if 'primeMeta' in jl:
            if 'Directors' not in jl['primeMeta']:
                dropped1 += 1
                continue
        else:
            if not 'productDetail' in jl:
                dropped2 += 1
                continue
            elif 'Director' in jl['productDetail']:
                # 有导演一定是电影
                pass
            elif 'Run time' in jl['productDetail']:
                # 有时长那么很可能是电影
                pass
            elif 'MPAA rating' in jl['productDetail'] and 's_medNotRated' not in jl['productDetail']['MPAA rating']:
                # 有分级很可能是电影
                pass
            else:
                dropped2 += 1
                continue

        fo.write(line)

print(f"Drop prime video: {dropped1}")
print(f"Drop normal video: {dropped2}")
