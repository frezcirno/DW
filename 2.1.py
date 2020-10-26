
with open('titles.jl', 'w', encoding='utf-8') as fo:
    for i in range(1, 6):
        with open('titles{}.jl'.format(i), encoding='utf-8') as f:
            for line in f:
                fo.write(line)
