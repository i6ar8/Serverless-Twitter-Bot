import json
from tweetNflAr import tweetNflAr
import os
def lambda_handler(event, context):
    #getting the consumer key, consumer_secret,access_token, and access_token_secret from the environment variables and sending it to the tweetNflAr constructor
    tweetNflAr(os.environ.get("consumer_key"),os.environ.get("consumer_secret"),os.environ.get("access_token"),os.environ.get("access_token_secret"))
    return {
        'statusCode': 200,
        
    }