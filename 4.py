import json


with open('data.jl', encoding='utf-8') as f, open('data_fixed.jl', 'w', encoding='utf-8') as fo:
    for line in f:
        jl = json.loads(line)

        if 'additionalOptions' in jl:
            additionalOptions = set(jl['additionalOptions'])
            otherFormat = set(jl['otherFormat'])

            additionalOptions.difference_update(otherFormat)

            jl['additionalOptions'] = list(additionalOptions)
            line = json.dumps(jl)+'\n'

        fo.write(line)
