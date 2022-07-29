import os
import re
import tweepy
class TwitterAuth:
    def __init__(self, consumer_key, consumer_secret_key, access_token, access_secret_token):
        self.consumer_key = consumer_key
        self.consumer_secret_key = consumer_secret_key
        self.access_token = access_token
        self.access_secret_token = access_secret_token
    def authentication(self):
        auth = tweepy.OAuth1UserHandler(self.consumer_key, self.consumer_secret_key, self.access_token, self.access_secret_token)
        api = tweepy.API(auth)
        return api


if __name__ == "__main__":
    twitter_ob = TwitterAuth(os.getenv('consumerkey'), os.getenv('consumersecretkey'), os.getenv('accesstoken'), os.getenv('accesssecrettoken'))
    tweets = twitter_ob.authentication().home_timeline()

    list2 = []
    for tweet in tweets:
        data = (tweet.user.screen_name, tweet.id, re.sub(r'https\S+', '', tweet.text), tweet.created_at.date(), tweet.created_at.time(), tweet.favorite_count, tweet.retweet_count, tweet.source, re.findall('(?:(?:https?):\/\/)?[\w/\-?=%.]+\.[\w/\-&?=%.]+', tweet.text))
        list2.append(data)
    for i in list2:
        print("\n")
        for j in i:
            print(j)