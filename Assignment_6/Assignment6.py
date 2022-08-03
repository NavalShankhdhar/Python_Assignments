from logging import exception
import os

import re
import tweepy
from fpdf import FPDF
from dotenv import load_dotenv

from ..Assignment_1.assignment1 import MySQL, PostGreSQL

load_dotenv()

class MysqlConnection(MySQL):

    def create_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS TWITTER_INFO(
                                    User_ID VARCHAR(255) ,
                                    User_name VARCHAR(20) NOT NULL,
                                    TWEET VARCHAR(255),
                                    Date VARCHAR(25),
                                    Time VARCHAR(25),
                                    Favorites INT,
                                    Retweets INT,
                                    Source VARCHAR(50),
                                    URL VARCHAR(255))
                            """)
        self.connect.commit()

    def insert_row(self, tweet):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute("""INSERT INTO TWITTER_INFO(User_ID, User_name, TWEET, Date, Time, Favorites, Retweets, Source, URL) 
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", tuple(tweet))
        self.connect.commit()
        self.cursor.close()


class PostgresqlConnection(PostGreSQL):
    # function to create table in MySQL database
    def create_table(self):
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS TWITTER_INFO(
                                    User_ID VARCHAR(255),
                                    User_name VARCHAR(20) NOT NULL,
                                    TWEET VARCHAR(255),
                                    Date VARCHAR(25),
                                    Time VARCHAR(25),
                                    Favorites INT,
                                    Retweets INT,
                                    Source VARCHAR(50),
                                    URL VARCHAR(255))
                            """)
        self.connect.commit()
    # function to insert data in MySQL table
    def insert_row(self, tweet):
        self.connect = self.connect_database()
        self.cursor = self.connect.cursor()
        self.cursor.execute("""INSERT INTO TWITTER_INFO(User_ID, User_name, TWEET, Date, Time, Favorites, Retweets, Source, URL)
                                VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", tuple(tweet))
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
            tweet_info = [
                tweet.id,
                tweet.user.screen_name,
                re.sub(r'https\S+', '', tweet.text),
                tweet.created_at.date(),
                tweet.created_at.time(),
                tweet.favorite_count,
                tweet.retweet_count,
                tweet.source,
                re.findall('(?:(?:https?):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+',tweet.text)
                ]
            # extract the URL out of the list's list
            if tweet_info[8]!= []:
                url = tweet_info[8][0]
            #if there is no URL accessible, empty list at URL's index is replaced by empty string
            else:
                url = ""
            tweet_info.pop()
            tweet_info.append(url)
            tweet_list.append(tweet_info)
        return tweet_list
    # function to extract tweet and it's info for pdf
    def extract_tweet_pdf(self, tweets):
        tweet_list = []
        for tweet in tweets:
            tweet_info =  [
                tweet.user.screen_name,
                re.sub(r'https\S+', '', tweet.text),
                tweet.favorite_count,
                tweet.retweet_count,
                re.findall('(?:(?:https?):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+', tweet.text)
                ]
            tweet_list.append(tweet_info)
        return tweet_list


class PDF(FPDF):
    #function to add header, it is automatically called when add_page() is callled
    def header(self):
        self.set_font("helvetica","B", 20)
        self.cell(self.epw, 0, "Tweet Info", 0, align='C')
        self.ln(20)
    # function to add footer, it is automatically called when close() is called
    def footer(self):
        self.ln(10)
        self.set_font("helvetica",'I', 12)
        self.cell(self.epw, 0, "Page" + str(self.page_no()) + "/{nb}", 0, align='C')
    # function to add and make the headings of table bold
    def render_table_header(self):
        self.set_font(style = "B")
        self.cell(30, self.line_height, self.table_col[0], border=1, align='C')
        self.cell(70, self.line_height, self.table_col[1], border=1, align='C')
        self.cell(20, self.line_height, self.table_col[2], border=1, align='C')
        self.cell(20, self.line_height, self.table_col[3], border=1, align='C')
        self.cell(50, self.line_height, self.table_col[4], border=1, align='C')
        self.ln(self.line_height)
        self.set_font(style = "")
    # function to create insert the data in table in pdf
    def create_pdf(self, data, table_col):
        self.table_col = table_col
        self.line_height = self.font_size * 5
        self.col_width = self.epw / len(table_col)
        self.render_table_header()
        for row in data:
            if self.will_page_break(self.line_height):
                self.render_table_header()
            self.multi_cell(30, self.line_height, str(row[0]), border=1, new_x="RIGHT", new_y="TOP", max_line_height=self.font_size)
            temp = row[1].encode('latin-1', 'replace').decode('latin-1')
            self.multi_cell(70, self.line_height, str(temp), border=1, new_x="RIGHT", new_y="TOP",max_line_height=self.font_size)
            self.multi_cell(20, self.line_height, str(row[2]), border=1, new_x="RIGHT", new_y="TOP", max_line_height=self.font_size)
            self.multi_cell(20, self.line_height, str(row[3]), border=1, new_x="RIGHT", new_y="TOP", max_line_height=self.font_size)
            self.multi_cell(50, self.line_height, str(row[4]), border=1, new_x="RIGHT", new_y="TOP", max_line_height=self.font_size)
            self.ln(self.line_height)
        self.output('Output.pdf')

if __name__ == "__main__":
    try:
        twitter_ob = TwitterAuth(os.getenv('consumerkey'), os.getenv('consumersecretkey'), os.getenv('accesstoken'), os.getenv('accesssecrettoken'))
        # object to connect to MySQL and PostGreSQL database
        mysql_ob = MysqlConnection(os.getenv("host"), os.getenv("mysqluser"), os.getenv("password"), 'MySQL_Games')
        psql_ob = PostgresqlConnection(os.getenv("host"), os.getenv("psqluser"), os.getenv("password"), 'allgames')

        mysql_ob.create_table()
        psql_ob.create_table()
        # extract tweets from home timeline of user
        tweets = twitter_ob.authentication().home_timeline()
        tweets_list = twitter_ob.extract_tweet(tweets)
        # insert tweet to mysql and postgresql database
        for tweet in tweets_list:
            mysql_ob.insert_row(tweet)
        for tweet in tweets_list:
            psql_ob.insert_row(tweet)

        pdf = PDF()
        pdf.add_page()
        pdf.set_font("Helvetica", size = 10)
        col_heading =("User Name", "Tweet", "Favorites", "Retweets", "URL")
        pdf.create_pdf( twitter_ob.extract_tweet_pdf(tweets), col_heading)
    except Exception as e:
        print(exception)