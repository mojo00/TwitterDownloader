TwitterDownloader
=================

Version 0.1
10/23/2013

About

TwitterDownloader is python class for downloading a large volume of tweets.  The queries are spaced out in time so that you won't run into query rate limiting errors.

Required packages:
- Twython
- numpy

Getting started:
1) Get authentication keys by registering your app at https://dev.twitter.com/apps.
2) Save the authentication information to a file with the text in json format (you can modify twitterAuthenticationExample.txt).
3) Modify and run Test_TwitterDownloader.py.

The tweets are saved to file in json format.