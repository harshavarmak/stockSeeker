
import requests
import json
import os
import sqlite3 as sl
from pprint import pprint as pp
import re
import string
import datetime
import time
from ssDataBaseObject import dbManager

pattern = re.compile(r'[\s]+', re.UNICODE)
DAYS_LIMIT = 2

def getCurrentPaginatedPost(srj, srUrl, after='', count=''):
    """
    Gets paginated data from input url 
            Parameters:
                    srj (string): sr contained data object
                    srUrl (string): url endpoint
                    after (type): paginated key to start
                    count (type): paginated count of results consumed
            Returns:
                    srj (dict): sr contained data object
                    after (string): paginated key to start
                    count (int): paginated count of results consumed
                    created (string): post created time
    """
    ssHeaders = {
        'User-Agent': 'stockSeeker v0.1',
        'From': 'unknown@stockSeeker.com',
        'X-Modhash': ''
    }
    srUrl += f'?after={after}&count={count}'
    currentPD = requests.get(srUrl, headers=ssHeaders, timeout=15)
    priorPostTooOld = False
    if(currentPD.status_code == 200):
        currentData = json.loads(currentPD.text)
        for j in currentData['data']['children']:
            if(int(j['data']['created_utc']) > int((datetime.datetime.utcnow() - datetime.timedelta(days=DAYS_LIMIT)).timestamp())):
                if(j['data']['selftext'] == ""):
                    continue
                srj[j['data']['name']] = [pattern.sub(' ', j['data']['subreddit']), pattern.sub(' ', j['data']['title']), pattern.sub(' ', j['data']['selftext']), j['data']['permalink'], str(j['data']['created_utc'])]
            else:
                if(priorPostTooOld):
                    return srj
                else:
                    priorPostTooOld = True
        return srj, currentData['data']['after'], currentData['data']['dist']
    else:
        return srj

def getCommentRepliesAsComment(pcj, repliesJ, postPermalink):
    for c in repliesJ['data']['children']:
        if(c['kind'] == "more"):
            commentListIds = c['data']['children']
            for comment in commentListIds:
                getCurrentPaginatedCommentFromPost(pcj, f'https://reddit.com/{postPermalink}{comment}/.json')
            continue
        if(c['data']['body'] == ""):
            if(c['data']['replies'] == ""):
                continue
            else:
                getCommentRepliesAsComment(pcj, c['data']['replies'], postPermalink)
        else:
            if(c['data']['body'] != "[deleted]" and c['data']['body'] != "[removed]"):
                pcj[c['data']['name']] = [pattern.sub(' ', c['data']['parent_id']), pattern.sub(' ', c['data']['body'])]
                if(c['data']['replies'] == ""):
                    continue
                else:
                    getCommentRepliesAsComment(pcj, c['data']['replies'], postPermalink)


def getCurrentPaginatedCommentFromPost(pcj, pcUrl):
    ssHeaders = {
        'User-Agent': 'stockSeeker v0.1',
        'From': 'unknown@stockSeeker.com',
        'X-Modhash': ''
    }
    pcUrlN = pcUrl
    currentPC = requests.get(pcUrlN, headers=ssHeaders, timeout=15)
    if(currentPC.status_code == 200):
        currentComments = json.loads(currentPC.text)
        postPermalink = currentComments[0]['data']['children'][0]['data']['permalink']
        for c in currentComments[1]['data']['children']:
            if(c['kind'] == "more"):
                commentListIds = c['data']['children']
                for comment in commentListIds:
                    getCurrentPaginatedCommentFromPost(pcj, f'https://reddit.com{postPermalink}{comment}/.json')
                continue
            if(c['data']['body'] == ""):
                if(c['data']['replies'] == ""):
                    continue
                else:
                    getCommentRepliesAsComment(pcj, c['data']['replies'], postPermalink)
            else:
                if(c['data']['body'] != "[deleted]" and c['data']['body'] != "[removed]"):
                    pcj[c['data']['name']] = [pattern.sub(' ', c['data']['parent_id']), pattern.sub(' ', c['data']['body'])]
                    if(c['data']['replies'] == ""):
                        continue
                    else:
                        getCommentRepliesAsComment(pcj, c['data']['replies'], postPermalink)


def getAndStorePostData(subreddit):
    """
    Grabs data from input subreddit
            Parameters:
                    subreddit (string): name of subreddit
            Returns:
                    srj (dict): json data of subreddit from api
    """
    srUrl = f'https://www.reddit.com/r/{subreddit}/.json'
    srj = dict()
    data = getCurrentPaginatedPost(srj, srUrl)
    while True:
        if(isinstance(data, tuple)):
            if(data[1] == None):
                return srj
            data = getCurrentPaginatedPost(data[0], srUrl, data[1], data[2])
        elif(isinstance(data, dict)):
            print('Got dict, discontinuing scraper')
            return srj
        else:
            print('Not tuple or dict. Breaking')
            break
    return srj

def getAndStoreCommentData(postPermalink):
    """
    Grabs data from subreddit post's comment
            Parameters:
                    sr (string): name of subreddit
                    postId (string): postId- same as SubredditStockData.id
            Returns:
                    pcj (dict): json data of post comments
    """
    pcUrl = f'https://www.reddit.com/{postPermalink}/.json'
    pcj = dict()
    getCurrentPaginatedCommentFromPost(pcj, pcUrl)
    return pcj

def getSubreddits(APP_LOC):
    """
    Gets list of subreddits from predefined file name
            Parameters:
                    APP_LOC (string): location of app respective to machine
            Returns:
                    subreddits (list): list of strings containing subreddit names
    """
    fileLoc = APP_LOC + '/files/subreddits.ssf'
    try:
        with open(fileLoc) as fileReader:
            subreddits = fileReader.read().splitlines()
        return subreddits
    except Exception as e:
        print(e)
        return False

if __name__ == "__main__":
    APP_LOC = os.getcwd()
    subreddits = getSubreddits(APP_LOC)
    print('Retrieved stocks related subreddits')
    pStart = time.time()
    pData = dict()
    
    for sr in subreddits[:1]:
        print('Scraping posts from -', sr)
        pData.update(getAndStorePostData(sr))
        print('Completed post scrape of -', sr)
    pEnd = time.time()
    print('Complted Post Scrape')
    ssDB = dbManager('test.db')
    ssDB.checkAndConnect()
    psStart = time.time()
    ingestRuntime = datetime.datetime.now().strftime('%D %H:%M')
    for post in pData:
        postData = [post]
        postData.extend(pData[post])
        postData.append(ingestRuntime)
        ssDB.insertData('SubRedditStockData', postData)
    psEnd = time.time()
    print('Completed Post Store')
    cStart = time.time()
    # cData = getAndStoreCommentData('stocks', 'kzq9zn')
    cData = dict()
    starterPerc = 5
    print(f'Getting Comments from {len(pData)} posts')
    for i, post in enumerate(pData):
        cData.update(getAndStoreCommentData(pData[post][3]))
        if(i == 1):
            continue
        if(int((i/len(pData))*100) == starterPerc):
            print(f'Completed {starterPerc}% of total posts.')
            starterPerc += 5
    cEnd = time.time()
    print('Completed Comment Scrape')
    csStart = time.time()
    for comment in cData:
        commentData = [comment]
        commentData.extend(cData[comment])
        commentData.append(ingestRuntime)
        ssDB.insertData('SubRedditPostComments', commentData)
    csEnd = time.time()
    print('Completed Comment Store')
    print('\nSTATS')
    print('Length of Post Dict: ', len(pData))
    print('Length of Comment Dict: ', len(cData))
    print('Posts Scraping time:', pEnd-pStart)
    print('Posts Storing time:', psEnd-psStart)
    print('Comments Scraping time:', cEnd-cStart)
    print('Comments Storing time:', csEnd-csStart)
    print('Total Running time:', csEnd-pStart)
    ssDB.closeDB()
    