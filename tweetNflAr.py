# -*- coding: utf-8 -*-
import tweepy
from nflops import proccommand,idDupChecker,uploadFile,getallid
import boto3
import pandas as pd

client=boto3.client("s3")
    
client.download_file('nflstats','id.csv','/tmp/id.csv')       






def tweetNflAr(consumer_key,consumer_secret,access_token,access_token_secret):
    twitter_auth_keys = {
        "consumer_key"        : consumer_key,
        "consumer_secret"     : consumer_secret,
        "access_token"        : access_token,
        "access_token_secret" : access_token_secret
    }
 
    auth = tweepy.OAuthHandler(
            twitter_auth_keys['consumer_key'],
            twitter_auth_keys['consumer_secret']
            )
    auth.set_access_token(
            twitter_auth_keys['access_token'],
            twitter_auth_keys['access_token_secret']
            )
    api = tweepy.API(auth ,wait_on_rate_limit=True)
 
  
    mentions=api.mentions_timeline(count=10)
    
    firsttimeGet=True
    firsttime=True
    ids=getallid(firsttimeGet)
    ids = list(filter(None, ids))
    firsttimeGet=False

    num=0
    errTxt="الرجاء اعادة كتابت السؤال بطريقة صحيحه"
    
    for mention in mentions:
        if(firsttime):
            ids=idDupChecker(ids,str(mention.id))
            firsttime=False
        else:
            ids=idDupChecker(ids[1],str(mention.id))
        num+=1
        print(num)
        
        isTrue=ids[0]

        if(isTrue!=True):
            tweet = proccommand(mention.text)
            try:
                if(tweet=="skip"):
                    print("skip")
                elif(tweet!="NULL"):
                    api.update_status(tweet,in_reply_to_status_id=str(mention.id),auto_populate_reply_metadata=True)
                else:
                    api.update_status(errTxt,in_reply_to_status_id=str(mention.id),auto_populate_reply_metadata=True)

            except Exception as err:
                print(err)

        
    uploadFile()

          
    
        
  
            
        

        
        

    
    

    

        
        























