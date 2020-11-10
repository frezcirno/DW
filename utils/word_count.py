# -*- coding：utf-8 -*-
# @Project : DW
# @File    : word_count.py
# @Time    : 2020/11/6 21:24
# @Author  : 刘文朔(liuwenshuo2000@126.com)
# @Software: IntelliJ IDEA

from pattern.text.en import *
import pandas as pd

if __name__ == '__main__':
    # 库本地运行有bug，先跑一下使其正常运作
    state = 1
    while (state):
        try:
            parsetree('"There Is So Much Darkness Now ~ Come For The Miracle"', relations=True, lemmata=True)
            state = 0
        except Exception:
            pass

    # 获取单词
    word_dict = {}
    result = {}
    with open('movies_out.txt', 'r') as f:
        str0 = f.readline()
        loop = 0
        while str0:
            loop += 1
            text = str0.strip().split()
            num = int(text[-1])
            del text[-1]
            for word in text:
                tree = parsetree(word, lemmata=True)
                for i in tree:
                    for lemma in i.lemma:
                        if lemma.isalpha():
                            word_dict.setdefault(lemma, 0)
                            word_dict[lemma] += num
            str0 = f.readline()

    print(loop)

    pd.DataFrame(word_dict, index=[1]).melt(var_name='word', value_name='count').to_csv('word_count.csv')

    # 处理剩余的一些没有合并的变形（现在动词变形的形容词和原词视为不同的词，此条作废）
    # pd.DataFrame(word_dict, index=[1]).melt(var_name='word', value_name='count').to_csv('raw_word_count.csv')
    # for i in word_dict.items():
    #     word = conjugate(i[0], "inf")
    #     result.setdefault(word,0)
    #     result[word] += i[1]
    # pd.DataFrame(result, index=[1]).melt(var_name='word', value_name='count').to_csv('word_count.csv')
