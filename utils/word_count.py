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
    with open('movies.txt', 'r', encoding="iso-8859-1") as f:
        while (f.readline().strip()):
            for i in range(5):
                while (f.readline()[0:7] != "review/"):
                    pass
            summary = f.readline().strip().split(maxsplit=1)[-1]
            text = f.readline().strip().split(maxsplit=1)[-1]
            summary = summary.replace("<br />", " ")
            text = text.replace("<br />", " ")
            s = parsetree(summary, relations=True, lemmata=True)
            t = parsetree(text, relations=True, lemmata=True)
            for words in t:
                for lemma in words.lemma:
                    if lemma.isalpha():
                        word_dict.setdefault(lemma, 0)
                        word_dict[lemma] += 1
            f.readline()

    pd.DataFrame(word_dict, index=[1]).melt(var_name='word', value_name='count').to_csv('word_count.csv')

    # 处理剩余的一些没有合并的变形（现在动词变形的形容词和原词视为不同的词，此条作废）
    # pd.DataFrame(word_dict, index=[1]).melt(var_name='word', value_name='count').to_csv('raw_word_count.csv')
    # for i in word_dict.items():
    #     word = conjugate(i[0], "inf")
    #     result.setdefault(word,0)
    #     result[word] += i[1]
    # pd.DataFrame(result, index=[1]).melt(var_name='word', value_name='count').to_csv('word_count.csv')
