import tweepy
import time
from os import environ as env

api_key = env.get("api_key")
api_key_secret = env.get("api_key_secret")
access_token = env.get("access_token")
access_token_secret = env.get("access_token_secret")
bearer_token = env.get("bearer_token")


client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key,api_key_secret,access_token,access_token_secret)
api = tweepy.API(auth)

class MyStream(tweepy.StreamingClient):
    def on_connect(self):
        print("Connected")

    def on_tweet(self, tweet):
        if tweet.reference_tweets == None:
            print(tweet.text)
            time.sleep(0.2)

stream = MyStream(bearer_token=bearer_token)
stream.add_rules(tweepy.StreamRule(["Blackpink"]))

stream.filter(tweet_fields=["reference_tweets"])

