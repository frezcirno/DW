import json

pidall = set()

with open('pid.txt') as f:
    for line in f:
        line = line.strip()
        pidall.add(line)

pid = dict()
with open('titles.jl', encoding='utf-8') as f:
    for line in f:
        jl = json.loads(line)
        tpid = jl['pid']
        if tpid in pid.keys():
            print('0')
            print(pid[tpid])
            print('1')
            print(line)
            if int(input('Choose from 0 and 1')) == 1:
                pid[tpid] = line
        else:
            pid[tpid] = line


with open('pid.txt') as f, open('data.jl', 'w', encoding='utf-8') as fo, open('pidmissing.txt', 'w') as fr:
    for line in f:
        line = line.strip()
        if line in pid.keys():
            oline = pid[line]
            fo.write(oline)
        else:
            fr.write(line+"\n")
