# -*- coding：utf-8 -*-
# @Project : DW
# @File    : clean_words.py
# @Time    : 2020/11/9 22:37
# @Author  : 刘文朔(liuwenshuo2000@126.com)
# @Software: IntelliJ IDEA

import unicodedata

if __name__ == '__main__':
    with open("movie_words.txt", encoding="iso-8859-1") as f, open("movies_out.txt", "w", encoding="utf-8") as fo:
        while True:
            ch = f.read(1)
            if ch == "<":
                while ch != "\t" and ch != ">":
                    ch = f.read(1)
                    if not ch: break
            if not ch: break
            if ch.isalnum() or ch == "\n" or ch == "'" or ch == "\t" or ch == "" or ch == "&":
                fo.write(ch)
            else:
                ch = unicodedata.normalize('NFKD', ch).encode('ascii', 'ignore').decode()
                if ch.isalnum():
                    fo.write(ch)
                else:
                    fo.write(' ')
