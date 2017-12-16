import MySQLdb
import tweepy
import sys
import jsonpickle
import json
import os
import datetime
import sys
import random
import math
import numpy as np
import pickle


# credentials from https://apps.twitter.com/
consumerKey = "BfZpE2cQGMOtGnGNICgi1tGmn"
consumerSecret = "VdC07qWSUPRomgewYHUmzXzhMFc722E3YbDw4jFiwmvFZ8ZsxX"

auth = tweepy.AppAuthHandler(consumerKey, consumerSecret)

api = tweepy.API(auth, wait_on_rate_limit=True,
                   wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)
else:
    print "Successful"

count = 0
sinceId = None
startDate = datetime.datetime(2015, 11, 10, 0, 0, 0)
endDate =   datetime.datetime(2015, 11, 17, 0, 0, 0)

foll_inf = 0.0
ret_inf = 0.0
fav_inf = 0.0
stdList = []

db = MySQLdb.connect("localhost", "root", "123xyz", "twitterDB")
cursor = db.cursor()

def activeUsers():
    activeUsers.counter += 1

activeUsers.counter = 0


def getActiveUsers(user_id):
    try:
        tmpTweet = api.user_timeline(id=user_id, count=1, include_rts=1)[0] #Fetches first entry, i.e. last tweet
    except tweepy.TweepError as e:
        activeUsers()
        print "Error: " + str(e)
        return
    except IndexError:
        return
    if tmpTweet.created_at < endDate and tmpTweet.created_at > startDate:
            activeUsers()
            print tmpTweet.user.screen_name + "    " + "Active user"
    return

def act_frnds_of_followers(count):
    activeFriends = 0.0
    i=0
    while i<10:
        perc = random.randint(50, 80)
        activeFriends += float(perc) / 100 * count
        i+=1
    activeFriends/=10
    return activeFriends

#sql = "select distinct user_id from htag5 where ret_inf is null order by foll_count asc"
sql = "select distinct user_id from htag5 where ret_inf is null order by rt_count desc"
cursor.execute(sql)
userIDs = cursor.fetchall()
print userIDs

for userID in userIDs:
    try:
        #foll_inf = ['foll_inf']
        user = userID[0]
        print user
        for foll in tweepy.Cursor(api.followers, id=user, count=200).items():
            getActiveUsers(foll._json['id'])
            print activeUsers.counter
            #print foll.followers_count
            actFrnds = act_frnds_of_followers(foll._json['friends_count'])
            stdList.append(foll._json['followers_count'])
            if (actFrnds == 0):
            	actFrnds = 1
            foll_inf += 1/actFrnds

        sql="update htag5 set foll_inf=('%.4f') where user_id=('%d')" % (foll_inf, user)
        cursor.execute(sql)
        db.commit()
        sql = "select tweet_id, rt_count, fav_count from htag5 where user_id='%d'" % (user)
        cursor.execute(sql)
        all_tweets = cursor.fetchall()
        #print all_tweets
        for tweet in all_tweets:
            print str(tweet[0]) + "  " + str(tweet[1]) + "    " + str(tweet[2])
            if(activeUsers.counter==0):
                ret_inf = 0.0
            else:
                ret_inf = tweet[1]/float(activeUsers.counter)

            fav_inf = tweet[2] * math.log(np.std(stdList), 10)
            sql="update htag5 set ret_inf=('%.4f'), fav_inf=('%.4f') where tweet_id=('%d')" % (ret_inf, fav_inf, tweet[0])
            cursor.execute(sql)
            db.commit()
        activeUsers.counter = 0
        del stdList[:]
        foll_inf = 0.0
        print "User Processed"
    except tweepy.error.TweepError as e:
        foll_inf = 0.0
        del stdList[:]
        activeUsers.counter = 0
        print "Process Later: " + str(e)
        continue


db.close()