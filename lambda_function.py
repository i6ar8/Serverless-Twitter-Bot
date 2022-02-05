import json
from tweetNflAr import tweetNflAr
import os
def lambda_handler(event, context):
    
    tweetNflAr(os.environ.get("consumer_key"),os.environ.get("consumer_secret"),os.environ.get("access_token"),os.environ.get("access_token_secret"))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }