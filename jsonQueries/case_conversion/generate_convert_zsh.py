#!/usr/bin/env python

def fix(word):
    ret = ""
    flag = False
    for c in word:
        if c == '_':
            flag = True
            continue
        if flag:
            c = c.upper()
            flag = False
        ret += c
    return ret
words = [line.strip() for line in open('keywords.txt', 'r') if len(line.strip())]
fixed = [fix(word) for word in words]

sed_dblqt_expr = [r"s#\"{}\"#\"{}\"#g".format(w,f) for w, f in zip(words, fixed)]
sed_sglqt_expr = [r"s#'{}'#'{}'#g".format(w,f) for w, f in zip(words, fixed)]

front = """for file in **/*.json **/*.py **/*.java
do
    sed -i bak -e \""""
middle = '\" \\\n       -e \"'.join(sed_dblqt_expr + sed_sglqt_expr)
end = """\" $file
    echo $file
done"""
print "%s%s%s" % (front, middle, end)
