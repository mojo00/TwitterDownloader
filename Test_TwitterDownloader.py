# -*- coding: utf-8 -*-
"""
Created on Sat Sep 14 14:45:33 2013

@author: jkwong
"""
#Test_TwitterDownloader.py

import TwitterDownloader

# Where you want to save the data
saveDirectory = r''
# Number of tweets to save per file
entriesPerFile = 10000
# total nubmer of tweets to download
count = 100000
# max id of tweet to save
max_id = 995940151530168320
max_id = 393059791402070015

# Try two ways of initialiing the TwitterDownloader Object

# Method 1- provide path to file
authenFilename = r''
twitter = TwitterDownloader.TwitterDownloader(authenFilename)

# Method 2- provide authentication strings
#twitter = TwitterDownloader.TwitterDownloader(consumer_key, consumer_secret, access_token_key, access_token_secret)

    
# Dump tweets to file
twitter.Dump(q = 'apple', dataPath = saveDirectory, entriesPerFile = entriesPerFile, count = count, max_id = max_id)


