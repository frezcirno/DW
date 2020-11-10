import csv


pId = set()
uId = set()
reviewCount = 0

with open('movies.txt', encoding="iso-8859-1") as f, open('product.csv', 'w', encoding='utf-8', newline='') as fproduct, open('user.csv', 'w', encoding='utf-8', newline='') as fuser, open('review.csv', 'w', encoding='utf-8', newline='') as freview:
    product = csv.writer(fproduct)
    user = csv.writer(fuser)
    review = csv.writer(freview)

    product.writerow(['productId'])
    user.writerow(['userId'])
    review.writerow(['productId', 'userId'])

    data = {}
    last = ''

    for (index, line) in enumerate(f):
        line = line.strip()

        if line == '':
            continue

        elif line.startswith('product/productId:'):

            if data['productId'] not in pId:
                pId.add(data['productId'])
                product.writerow([data['productId']])

            if data['userId'] not in uId:
                uId.add(data['userId'])
                user.writerow([data['userId']])

            review.writerow([data['productId'], data['userId']])
            reviewCount += 1

            value = line.split('product/productId:')[1].strip()
            data = {'productId': value}
            last = 'productId'

        elif line.startswith('review/userId:'):
            value = line.split('review/userId:')[1].strip()
            data['userId'] = value
            last = 'userId'

        elif line.startswith('review/profileName:'):
            value = line.split('review/profileName:')[1].strip()
            data['profileName'] = value
            last = 'profileName'

        elif line.startswith('review/helpfulness:'):
            value = line.split('review/helpfulness:')[1].strip()
            data['helpfulness'] = value
            last = 'helpfulness'

        elif line.startswith('review/score:'):
            value = line.split('review/score:')[1].strip()
            data['score'] = value
            last = 'score'

        elif line.startswith('review/time:'):
            value = line.split('review/time:')[1].strip()
            data['time'] = value
            last = 'time'

        elif line.startswith('review/summary:'):
            value = line.split('review/summary:')[1].strip()
            data['summary'] = value
            last = 'summary'

        elif line.startswith('review/text:'):
            value = line.split('review/text:')[1].strip()
            data['text'] = value
            last = 'text'

        else:
            data[last] += ' ' + line
            print(index, 'Parse as', last)

    if data['productId'] not in pId:
        pId.add(data['productId'])
        product.writerow([data['productId']])

    if data['userId'] not in uId:
        uId.add(data['userId'])
        user.writerow([data['userId']])

    review.writerow([data['productId'], data['userId']])
    reviewCount += 1

print('Fin.')
print('Number of products:', len(pId))
print('Number of users:', len(uId))
print('Number of reviews:', reviewCount)

# 多字段
# excel矩阵
# 有很多关联
# 9:00-5:30
# 1-5