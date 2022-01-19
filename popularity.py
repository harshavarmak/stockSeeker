import requests
import json
import os
import sqlite3 as sl
from pprint import pprint as pp
import re
import string
import datetime
import time
import pandas as pd
from ssDataBaseObject import dbManager

def readListedStocks(APP_LOC):
    fileLoc = APP_LOC + '/files/nasdaqtraded.txt'
    df = pd.read_csv(fileLoc, sep='|', header=0)
    df.drop('Nasdaq Traded', inplace=True, axis=1)
    df.drop('Listing Exchange', inplace=True, axis=1)
    df.drop('Market Category', inplace=True, axis=1)
    df.drop('Round Lot Size', inplace=True, axis=1)
    df.drop('Test Issue', inplace=True, axis=1)
    df.drop('CQS Symbol', inplace=True, axis=1)
    df.drop('Symbol', inplace=True, axis=1)
    df.drop('NextShares', inplace=True, axis=1)
    df.drop('Financial Status', inplace=True, axis=1)
    df.rename(columns={'NASDAQ Symbol':'Symbol'}, inplace=True)
    return df

def findWholeWord(w):
    # return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search
    return re.compile(r'\b({0})\b'.format(w)).search
    #return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE)

def commonMisfires():
    return ['AT', 'PSA', 'BE', 'YOLO', 'SEC', 'LEAP', 'EDIT', 'IMO', 'OR', 'DD', 'EV', 'A', 'R', 'CEO', 'EPS', 'IPO', 'ITM', 'OTM', 'GME', 'RH', 'UK', 'TD']

if __name__ == "__main__":
    APP_LOC = os.getcwd()
    listedStocks = readListedStocks(APP_LOC)
    compiledStocks = ''
    for s in listedStocks['Symbol'].tolist():
        compiledStocks += str(s) + '|'
    compiledStocks = compiledStocks[:-1]
    ssDB = dbManager('test.db')
    popularSymbols = dict()
    if(ssDB.checkAndConnect()):
        stockPosts = ssDB.readData('SubRedditStockData')
        pStart = time.time()
        for post in stockPosts:
            matchT = findWholeWord(compiledStocks)(str(post[2]))
            matchP = findWholeWord(compiledStocks)(str(post[3]))
            symbolsMatched = list()
            if(matchT is not None or matchP is not None):
                if(matchT is not None):
                    symbolsMatched.extend(list(matchT.groups()))
                if(matchP is not None):
                    symbolsMatched.extend(list(matchP.groups()))
                for symbol in list(set(symbolsMatched)):
                    if symbol.strip('$') in popularSymbols:
                        popularSymbols[symbol] += popularSymbols[symbol] + 1
                    else:
                        popularSymbols[symbol] = 1
        pEnd = time.time()
        commentPosts = ssDB.readData('SubRedditPostComments')
        cStart = time.time()
        for comment in commentPosts:
            matchC = findWholeWord(compiledStocks)(str(comment[2]))
            symbolsMatched = list()
            if(matchC is not None):
                symbolsMatched.extend(list(matchC.groups()))
            for symbol in list(set(symbolsMatched)):
                if symbol.strip('$') in popularSymbols:
                    popularSymbols[symbol] += popularSymbols[symbol] + 1
                else:
                    popularSymbols[symbol] = 1
        cEnd = time.time()
    popularSymbols = dict([(key, val) for key, val in popularSymbols.items() if val != 1 if key not in commonMisfires()]) 
    # pp(popularSymbols)
    from collections import OrderedDict
    ordered = OrderedDict(sorted(popularSymbols.items(), key=lambda v: v[1]))
    pp(ordered)
    print('\nSTATS')
    print('Post Matcher Processing Time:', pEnd-pStart)
    print('Comment Matcher Processing Time:', cEnd-cStart)
    print('Size of Matched Stocks:', len(popularSymbols))