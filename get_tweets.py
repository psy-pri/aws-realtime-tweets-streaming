"""
Author: Priyanka Shukla
This script generates a stream of tweets using Twitter API v1.1, 
adds it to AWS Kinesis Firehose Stream and ingests the 
data into Snowflake via Snowpipe
"""

import tweepy
import os
from dotenv import load_dotenv
import boto3
import json

#load environment variables
load_dotenv()
api_key = os.getenv("api_key")
api_key_secret = os.getenv("api_key_secret")
access_token = os.getenv("access_token")
access_token_secret = os.getenv("access_token_secret")
bearer_token = os.getenv("bearer_token")
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")

#create boto3 session instance
session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name='us-east-2'
)

#create kinesis firehose client instance
kinesis_client = session.client(service_name='firehose')

def sendDataToStream(data):
    """
    This function will send tweets to 
    Kinesis firehose stream
    :param data: stream of tweets of type bytes
    :return response: dict object
    """
    delimiter = "\n"
    data = data.decode('utf8').replace("'", '"')
    response = kinesis_client.put_record(
        DeliveryStreamName = 'PUT-S3-sTkYd',
        Record = {
            "Data":json.dumps(data) + delimiter
        }
    )
    return response


auth = tweepy.OAuth1UserHandler(consumer_key=api_key,consumer_secret=api_key_secret,
        access_token=access_token,access_token_secret=access_token_secret)

api = tweepy.API(auth)

search_terms = ["blackpink"]

class MyStream(tweepy.StreamingClient):
    """
    This class customizes the processing of the stream data, 
    StreamingClient needs to be subclassed
    """
    def on_connect(self):
        """
        This function alerts if connection 
        was established successfully
        """
        print("Connected")

    def on_data(self, data):
        """
        This function checks if data was fetched successfully
        and send data to the Kinesis Firehose stream
        """
        try:
            print("Data fetched successfully",data[0])
            response = sendDataToStream(data)
            # data = data.decode('utf-8') 
            # data = {"data": data.decode('utf-8')}
            # with open('request.txt', 'w') as file:
            #     json.dump(data, file) + ","
        except BaseException as e:
            print("Error on_data %s" % str(e))
        return True

    def on_error(self, status):
        """
        This function prints the 
        status in case of any failure
        """
        print("status:",status)

    def on_connection_error(self):
        """
        This function stops the connection 
        in case of connection issues
        """
        self.disconnect()

#initialize streamingclient object with bearer token
stream = MyStream(bearer_token=bearer_token)

for term in search_terms:
    #add rules before running filtered stream
    stream.add_rules(tweepy.StreamRule(term))

#connect to and run the stream
stream.filter()


