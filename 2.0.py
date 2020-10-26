

with open('pid.txt', encoding='utf-8') as f, open('pid1.txt', 'w', encoding='utf-8') as f1, open('pid2.txt', 'w', encoding='utf-8') as f2, open('pid3.txt', 'w', encoding='utf-8') as f3, open('pid4.txt', 'w', encoding='utf-8') as f4, open('pid5.txt', 'w', encoding='utf-8') as f5:
    for i in range(50000):
        f1.write(f.readline())
    for i in range(50000):
        f2.write(f.readline())
    for i in range(50000):
        f3.write(f.readline())
    for i in range(50000):
        f4.write(f.readline())
    for i in range(100000):
        f5.write(f.readline())
