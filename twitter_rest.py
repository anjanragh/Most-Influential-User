#!/usr/bin/python
import MySQLdb
import tweepy
import sys
import jsonpickle
import json
import os

# Replace the API_KEY and API_SECRET with your application's key and secret.
API_KEY = "BfZpE2cQGMOtGnGNICgi1tGmn"
API_SECRET = "VdC07qWSUPRomgewYHUmzXzhMFc722E3YbDw4jFiwmvFZ8ZsxX"

auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

api = tweepy.API(auth, wait_on_rate_limit=True,
				   wait_on_rate_limit_notify=True)

if (not api):
    print ("Can't Authenticate")
    sys.exit(-1)
else:
	print "Successful"


# Continue with rest of code
searchQuery = '#meditation'  # this is what we're searching for
maxTweets = 5000 # Some arbitrary large number
tweetsPerQry = 100  # this is the max the API permits
fName = 'tweets.json' # We'll store the tweets in a text file.


# If results from a specific ID onwards are reqd, set since_id to that ID.
# else default to no lower limit, go as far back as API allows
sinceId = None

# If results only below a specific ID are, set max_id to that ID.
# else default to no upper limit, start from the most recent tweet matching the search query.
max_id = -5L

# Tweets until the below date (YYYY-MM-DD)
date = "2015-11-14"


db = MySQLdb.connect("localhost", "root", "123xyz", "twitterDB")
cursor = db.cursor()

tweetCount = 0
print("Downloading max {0} tweets".format(maxTweets))
with open(fName, 'w') as f:
    while tweetCount < maxTweets:
        try:
            if (max_id <= 0):
                if (not sinceId):
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry, until=date)
                else:
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            since_id=sinceId, until=date)
            else:
                if (not sinceId):
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            max_id=str(max_id - 1), until=date)
                else:
                    new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                            max_id=str(max_id - 1),
                                            since_id=sinceId, until=date)
            if not new_tweets:
                print("No more tweets found")
                break
            for tweet in new_tweets:
                if(tweet._json['text'].find('RT ') == -1):
                    if (tweet._json['user']['followers_count'] <= 500) and (tweet._json['retweet_count'] >= 1) and (tweet._json['favorite_count'] >= 1):
	                    #print "lalala"
	                    f.write(jsonpickle.encode(tweet._json, unpicklable=False) + '\n')
	                    userId = tweet._json['user']['id']
	                    tweetId = tweet._json['id']
	                    userName = tweet._json["user"]["screen_name"]
	                    tweetRetCount = tweet._json["retweet_count"]
	                    tweetFavCount = tweet._json["favorite_count"]
	                    userFollCount = tweet._json["user"]["followers_count"]
	                    userFrndCount = tweet._json["user"]["friends_count"]
	                    createdAt = tweet._json["created_at"]
	                    actualDate = "2015-11-"+ createdAt[8:10] + " " + createdAt[11:19]
	                    sql = 'insert into htag5(tweet_id, user_id, scr_name, rt_count, fav_count, foll_count, frnd_count, created_at) values ("%d", "%d", "%s", "%d", "%d", "%d", "%d", "%s")' % (tweetId, userId, userName, tweetRetCount, tweetFavCount, userFollCount, userFrndCount, actualDate)
	                    try:
	                        cursor.execute(sql)
	                        db.commit()
	                    except:
	                        print "Error"
	                        db.rollback()
            tweetCount += len(new_tweets)
            print "Downloaded {0} tweets".format(tweetCount)
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))
            break

#print ("Downloaded {0} tweets, Saved to {1}".format(tweetCount, fName))

db.close()