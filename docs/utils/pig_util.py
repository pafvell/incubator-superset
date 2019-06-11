#! /usr/bin/env python
#coding=utf-8


import re


def parsePigBag(s):
    ret = []
    tuples = re.findall('(\(.*?\))', s)
    for tup in tuples:
        tmp = tup[1:-1].split(',')
        ret.append(tmp)
    return ret
