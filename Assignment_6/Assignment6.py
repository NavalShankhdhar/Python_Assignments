import os

from logging import exception

import re
import tweepy
from dotenv import load_dotenv
from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

class MysqlConnection(MySQL):
    def create_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS TWITTER_INFO7(User_ID VARCHAR(255) , User_name VARCHAR(20) NOT NULL, TWEET VARCHAR(255), Date VARCHAR(25), Time VARCHAR(25), Favorites INT, Retweets INT, Source VARCHAR(50), URL VARCHAR(255))")
        self.connect.commit()
    def insert_row(self, tweet):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute('INSERT INTO TWITTER_INFO7(User_ID, User_name, TWEET, Date, Time, Favorites, Retweets, Source, URL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', tuple(tweet))
        self.connect.commit()
        self.cursor.close()

class PostgresqlConnection(PostGreSQL):
    # function to create table in MySQL database
    def create_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS TWITTER_INFO7(User_ID VARCHAR(255) , User_name VARCHAR(20) NOT NULL,TWEET VARCHAR(255), Date VARCHAR(25), Time VARCHAR(25), Favorites INT, Retweets INT, Source VARCHAR(50), URL VARCHAR(255))")
        self.connect.commit()
    # function to insert data in MySQL table
    def insert_row(self, tweet):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute('INSERT INTO TWITTER_INFO7(User_ID, User_name, TWEET, Date, Time, Favorites, Retweets, Source, URL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)', tuple(tweet))
        self.connect.commit()
        self.cursor.close()

class TwitterAuth:
    # initializing authentication keys
    def __init__(self, consumer_key, consumer_secret_key, access_token, access_secret_token):
        self.consumer_key = consumer_key
        self.consumer_secret_key = consumer_secret_key
        self.access_token = access_token
        self.access_secret_token = access_secret_token
    # function to authenticate and connect to twitter account
    def authentication(self):
        auth = tweepy.OAuth1UserHandler(self.consumer_key, self.consumer_secret_key, self.access_token, self.access_secret_token)
        api = tweepy.API(auth)
        return api
    # function to extract tweet and it's info
    def extract_tweet(self, tweets):
        tweet_list = []
        for tweet in tweets:
            tweet_info = [tweet.id, tweet.user.screen_name, re.sub(r'https\S+', '', tweet.text), tweet.created_at.date(), tweet.created_at.time(), tweet.favorite_count, tweet.retweet_count, tweet.source, re.findall('(?:(?:https?):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+', tweet.text)]
            # extract the URL out of the list
            if tweet_info[8]!= []:
                url = tweet_info[8][0]
            #if there is no URL accessible, empty list at URL's index is replaced by empty string
            else:
                url = ""
            tweet_info.pop()
            tweet_info.append(url)
            tweet_list.append(tweet_info)
        return tweet_list

if __name__ == "__main__":
    twitter_ob = TwitterAuth(os.getenv('consumerkey'), os.getenv('consumersecretkey'), os.getenv('accesstoken'), os.getenv('accesssecrettoken'))
    # object to connect to MySQL and PostGreSQL database
    mysql_ob = MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
    psql_ob = PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')
    try:
        mysql_ob.create_table()
        psql_ob.create_table()
        # extract tweets from home timeline of user
        tweets = twitter_ob.authentication().home_timeline()
        tweets_list = twitter_ob.extract_tweet(tweets)
    
        for tweet in tweets_list:
            mysql_ob.insert_row(tweet)

        for tweet in tweets_list:
            psql_ob.insert_row(tweet)
    except Exception:
        print(exception)