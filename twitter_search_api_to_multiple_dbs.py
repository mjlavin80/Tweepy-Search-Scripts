#twitter_search_api
import time
import tweepy
import urllib
import json
import sqlite3
import os

tic = time.clock()

base_dir = os.getcwd()

query = urllib.quote_plus(raw_input("What key words would you like to search on Twitter? "))

#setting up the keys
consumer_key = '//ADD YOUR KEY HERE'
consumer_secret = '//ADD YOUR KEY HERE'
access_token = '//ADD YOUR KEY HERE'
access_token_secret = '//ADD YOUR KEY HERE'

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

"""Begin functions"""
def database_name_maker(database_name):   
    #make database and main table
    database_name = database_name + "_"+ str(time.strftime("%Y_%b_%d_%H_%M_%S") + ".db")
    return database_name

def database_maker(database_name):
    db = sqlite3.connect(database_name)
    db.execute('DROP TABLE IF EXISTS tweets')
    db.execute('CREATE TABLE tweets(data_as_dict BLOB)')
    db.commit()
    db.close()

def row_creator(database_name, row_dict_as_string):
    db = sqlite3.connect(database_name)
    db.execute('INSERT INTO tweets VALUES (?)', (row_dict_as_string,))
    #add tweet._json to db
    db.commit()
    db.close()

def tweet_handler(tweet):
    tweets_full.write(str(tweet._json) + "\n")
    # for i in tweet_list
    # grab data and write to database

count = 0
# do a loop here to gather by day for eight days

db_name = database_name_maker(query)
db_folder = db_name.replace(".db", "")
if not os.path.exists("./" + db_folder):
    os.makedirs("./" + db_folder)
os.chdir("./" + db_folder)
database_maker(db_name)
os.chdir(base_dir)

tweepy_cursor = tweepy.Cursor(api.search,
                           q=query,
                           count=100,
                           result_type="recent",
                           include_entities=True,
                           lang="en").pages()
"""
while True:
    count = 1
    for page in tweepy_cursor:
        for tweet in page:
            print count
            count +=1
    break
"""
while True:
    try:
        for tweets in tweepy_cursor:
            try:
                for tweet in tweets:
                    try:
                        count +=1
                        if count % 1000 == 0:
                            print(str(count) + " Tweets so far")
                        
                        if count % 10000 == 0:                   
                            os.chdir(base_dir)
                            os.chdir("./" + db_folder)
                            sqlite3.connect(db_name).close()
                            db_name = database_name_maker(query)
                            database_maker(db_name)
                            os.chdir(base_dir)
                        
                        os.chdir(base_dir)
                        os.chdir("./" + db_folder)
                        row_creator(db_name, str(tweet._json))
                        os.chdir(base_dir)
                        
                    except tweepy.TweepError:
                        print "Timeout occurred. Waiting fifteen minutes."
                        time.sleep(60 * 15)
                        continue
                    except tweepy.error.TweepError:
                        print "Timeout occurred. Waiting fifteen minutes."
                        time.sleep(60 * 15)
                        continue
                    except StopIteration:
                        break
        
            except tweepy.TweepError:
                print "Timeout occurred. Waiting fifteen minutes."
                time.sleep(60 * 15)
                continue
            except tweepy.error.TweepError:
                print "Timeout occurred. Waiting fifteen minutes."
                time.sleep(60 * 15)
                continue
            except StopIteration:
                break
        break    
    except tweepy.TweepError:
        print "Timeout occurred. Waiting fifteen minutes."
        time.sleep(60 * 15)
        continue
    except tweepy.error.TweepError:
        print "Timeout occurred. Waiting fifteen minutes."
        time.sleep(60 * 15)
        continue
    except StopIteration:
        break
    
print("Processed " + str(count) + " Tweets")