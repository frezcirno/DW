import csv
import json

pidok = set()

with open('movie.jl', encoding='utf-8') as f:
    for line in f:
        jl = json.loads(line)
        pidok.add(jl['pid'])

with open('movie.jl', encoding='utf-8') as f, open('node.csv', 'w', encoding='utf-8', newline='') as fo, open('otherFormat.csv', 'w', encoding='utf-8', newline='') as fo1, open('additionalOptions.csv', 'w', encoding='utf-8', newline='') as fo2:
    fcsv = csv.writer(fo)
    fcsv.writerow(['label', 'title'])
    fcsv1 = csv.writer(fo1)
    fcsv1.writerow(['label1', 'label2'])
    fcsv2 = csv.writer(fo2)
    fcsv2.writerow(['label1', 'label2'])

    for line in f:
        jl = json.loads(line)
        pid = jl['pid']
        fcsv.writerow([pid, jl['title']])
        for otherFormat in jl['otherFormat']:
            if otherFormat not in pidok:
                fcsv.writerow([otherFormat, 'others'])
                pidok.add(otherFormat)
            fcsv1.writerow([pid, otherFormat])
        if 'primeMeta' not in jl:
            if 'additionalOptions' in jl:
                for additionalOptions in jl['additionalOptions']:
                    if additionalOptions not in pidok:
                        fcsv.writerow([additionalOptions, 'others'])
                        pidok.add(additionalOptions)
                    fcsv2.writerow([pid, additionalOptions])
