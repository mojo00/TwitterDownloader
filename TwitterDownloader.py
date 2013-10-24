# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 20:09:38 2013
Updated 10/23/2013

@author: jkwong
"""
from twython import Twython, TwythonError
import numpy as np
import datetime
import time
import json
import os
import types
import codecs

class TwitterDownloader:
    """Class to facilitate streaming or downloading historical tweets to memory or file.
    """
    def __init__(self, *arg):
        """Provide filename of the json file containing the authentication information or provide the authentication strings directly"""
        if len(arg) == 1:  # If only one argument, then we are assuming that you are providing a file name tring
            authenFile = arg[0]
            """Read in the authentication information from a text file in json format. See twitterAuthenticationExample.txt"""
            try:
                print('Reading file: %s' %authenFile)
                with open(authenFile, 'rb') as fid:
                    lineIn = fid.readline()
                    self.authen = json.loads(lineIn)
                    fid.close()
            except:
                print "Error reading file"
                return
        else:
            self.authen = {}
            self.authen['consumer_key'] = arg[0]
            self.authen['consumer_secret'] = arg[1]
            self.authen['access_token_key'] = arg[2]
            self.authen['access_token_secret'] = arg[3]
        self.MAXTRIES = 5
        
    def Search(self, **kwargs):
        """
        Returns tweets of given search query.  This does not save tweets to file.
        You must provide at least one search query and the argument name has to be
        given (e.g. Search(q = "obama", ...))

        Arguments:
            :param dataPath [string] - path where you want the data fils stored
            :param fileNamePrefix [string] - this is added to the front of the file name
            :param entriesPerFile [int] - number of tweets to save per file
            and standard search parameters *

        Return:
            Nothing
            
        * https://dev.twitter.com/docs/api/1.1/get/search/tweets
        
        """
        
        searchParameterKeyList = ['q', 'geocode', 'result_type', 'until', 'max_id', 'count', 'lang', 'locale', 'since_id', 'include_entities']
        
        # Set default wait time between queries if no specified
        if 'waitTime' in kwargs.keys():
            waitTime = kwargs['waitTime']
        else:
            waitTime = 5.0
        # Set default number of tweets to retrieve
        if 'count' not in kwargs.keys():
            kwargs['count'] = 100
        
        twitter = Twython(self.authen['consumer_key'], self.authen['consumer_secret'], \
            self.authen['access_token_key'], self.authen['access_token_secret'])
        
        # Container for all search statuses
        search_statuses = []
        search_metadata = []
    
        while (kwargs['count'] > 0):
            
            # Bulid the query string
            paramString = ""
            for (index, searchParameterKey) in enumerate(searchParameterKeyList):
                if searchParameterKey in kwargs.keys() and kwargs[searchParameterKey] != None:
                    paramString = paramString + '%s = "%s",' %(searchParameterKey, kwargs[searchParameterKey])
            # Removing the trailing comma
            if (paramString[-1] == ','):
                paramString = paramString[0:-1]
    
            # Generate eval string
            evalStr = 'twitter.search(%s)' % paramString
            
            # Keep trying until get a result
            tries = 0
            while (tries < self.MAXTRIES):
                try:
                    search_results = eval(evalStr)
                    if search_results != []:
                        break
                except TwythonError as e:
                    tries += 1
                    print e
                    time.sleep(waitTime*2)
            
            # Break if no results were returned
            if (search_results['statuses'] == []):
                break
                
            # Append to list
            for n in search_results['statuses']:
                search_statuses.append(n)
            search_metadata.append(search_results['search_metadata'])
            
            # Display the first message every time we receive a block of messages
            if (search_results['statuses'] != []):
                tweet = search_results['statuses'][0]
                if ('text' in tweet.keys()):
                    print('Tweet from @%s Date: %s' % (tweet['user']['screen_name'].encode('utf-8'), tweet['created_at']))
                    print('Tweet text: %s' %tweet['text'].encode('utf-8'))
            # Print status report
            print('Requests left: %s, reset time: %s' %(twitter.get_lastfunction_header('X-Rate-Limit-Remaining'), \
                twitter.get_lastfunction_header('X-Rate-Limit-Reset')))
            
            # decrement the count; twitter API limits number of returned tweets to 100
            kwargs['count'] -= 100
    
            # Set new max_id so that don't get the same results again
            kwargs['max_id'] = search_results['statuses'][-1]['id']-1
    
            # Wait so we don't get the 420 error by making too many queries
            time.sleep(waitTime)
            
        return(search_statuses, search_metadata)

##    def Search(self, q='', geocode='', result_type='', until='', max_id = 99999999999999999999L, numberResults = 100, waitTime = 5.0):

    def Dump(self,**kwargs):
        """
        Uses Search to retrieve tweets and save to file in json format.
        The arguments are the standard query parameters as described in the twitter
        API *.  At least one of these search queries must be specified.  For an example
        on usage, look at Test_TwitterDownloader.py
            
        Arguments:
            :param dataPath [string] - path where you want the data fils stored
            :param fileNamePrefix [string] - this is added to the front of the file name
            :param entriesPerFile [int] - number of tweets to save per fil
            :param waitTime [int] - wait time in seconds between queries; used by Search
            and standard search parameters*
            
        Return:
            Nothing
        
        Output file
        The file name format is
        filename = '<1>_<2>_<3>_<4>_<5>_<6>_<7>_<8>_<9>_<10>_<11>.txt'
        1) fileNamePrefix
        2) date_time stamp: yyyymmdd_HHMMSS
        3) q
        4) geocode
        5) result_type
        6) until
        7) lang
        8) locale
        9) maximum id of tweets in file
        10) minimum id of tweets in file
        11) number of tweets in file
        
        For example
        >>  Search(dataPath = r'/Users/Admin/Documents/Data/twitterData', q = 'apple', entriesPerFile = 1000, count = 10000)
        This will retrieve 10,000 tweets that mention 'apple' and save them to files in
        /Users/Admin/Documents/Data/twitterData, with each file containing 1,000 tweets.
        
        * Twitter API:
        https://dev.twitter.com/docs/api/1.1/get/search/tweets
        
        """
        
        # default location for dumping data files\
        if 'dataPath' in kwargs.keys():
            dataPath = kwargs['dataPath']
        else:
            dataPath = '.'
        
        if 'fileNamePrefix' in kwargs.keys():
            fileNamePrefix = kwargs['fileNamePrefix']
        else:
            fileNamePrefix = None
            kwargs['fileNamePrefix'] = None
            
        if 'entriesPerFile' in kwargs.keys():
            entriesPerFile = kwargs['entriesPerFile']
        else:
            entriesPerFile = 1000

        if 'count' in kwargs.keys():
            totalCount = kwargs['count']
        else:
            totalCount = 1000

        searchParameterKeyList = ['q', 'geocode', 'result_type', 'until', 'max_id', \
            'count', 'lang', 'locale', 'since_id', 'include_entities']

        # Extract the search paramters
        print kwargs.keys()
        searchParameters = {}
        for s in searchParameterKeyList:
            if s in kwargs.keys():
                searchParameters[s] = kwargs[s]
            else:
                searchParameters[s] = None
                
        # Determine number of files
        numberFiles = int(np.ceil(totalCount / entriesPerFile))
        
        # Will grab this many tweets with each Search call
        searchParameters['count'] = entriesPerFile
        
        for num in xrange(numberFiles):
            
            # Get start time of the search
            timeNow = datetime.datetime.now()
            timeNowStr = timeNow.strftime('%Y%m%d_%H%M%S')
            
            # Bulid the query string
            paramString = ""
            for (index, searchParameterKey) in enumerate(searchParameterKeyList):
                if searchParameterKey in searchParameters.keys() and searchParameters[searchParameterKey] != None:
                    if isinstance(searchParameters[searchParameterKey], str):
                        paramString = paramString + '%s = "%s",' %(searchParameterKey, searchParameters[searchParameterKey])
                    else:
                        paramString = paramString + '%s = %d,' %(searchParameterKey, searchParameters[searchParameterKey])
                        
            # Removing the trailing comma
            if (paramString[-1] == ','):
                paramString = paramString[0:-1]
    
            # Generate eval string
            evalStr = 'self.Search(%s)' % paramString

            (search_statuses, search_metadata) = eval(evalStr)
            
            # break if no results
            if (search_statuses == []):
                break
        
            # Get max and min id's in statuses
            max_id_seen = search_statuses[0]['id']
            min_id_seen = search_statuses[-1]['id']
                        
            # Create the file name
            filename = '%s_%s_%s_%s_%s_%s_%s_%s_%d_%d_%d.txt' %(fileNamePrefix, \
                timeNowStr, searchParameters['q'], searchParameters['geocode'], \
                searchParameters['result_type'], searchParameters['until'], \
                searchParameters['lang'], searchParameters['locale'], \
                max_id_seen, min_id_seen, len(search_statuses))
            
            # Remove the underscore in front if present (no fileNamPrefix specified)
            if (filename[0] == '_'):
                filename = filename[1:]
            fullFilename = os.path.join(dataPath, filename)
            
            # Opens file that will hold the tweets
            with open(fullFilename, 'wb') as fid:
                # Convert each tweet to json text and writes to file
                i = 0
                for tweet in search_statuses:
                    fid.write(json.dumps(tweet) + '\n')
                    i = i + 1
                print 'Wrote %d entries to %s' %(i, fullFilename)
                fid.close()
            
            # Set maximum id for next iteration
            searchParameters['max_id'] = search_statuses[-1]['id']-1

def ReadDumpFiles(filenameList, combine = False):
    """Reads a list of twitter dump files given a list of filenames.
    Arguments:
        :param filenameList - List of filenames.  You can also provide a single filename as a string
        :param combine (optional) - True - combines all the tweets into a single list
                                  - False - the output will have one element per file
    Return:
        List of tweets
        
    """
    # Checks to see if the input is a string, indicatin single file
    #  If it is, convert to list
    if isinstance(filenameList, types.StringTypes):
        filenameList = [filenameList]
        combine = True
    outputAll = []
    for i in xrange(len(filenameList)):
        with open(filenameList[i], 'rb') as fid:
            lines = fid.readlines()
            if not combine:
                output = []
            for lineIn in lines:
                if not combine:
                    output.append(json.loads(lineIn))
                else:
                    outputAll.append(json.loads(lineIn))
            if not combine:
                outputAll.append(output)
    return(outputAll)
    
def RawTextDump(filenameList, outputFileName, minId = 0, maxId = 990684325412364289L):
    """Dumps all 'text' values from all the tweets of a list of files (filenameList) into
       a single file (outputFilename).  You can specify the minId and maxId of tweets to include.
    Arguments:
        :param filenameList - list of file names.
        :param outputFilename - name of file to write to
        :para minId (optional) - minimum tweet id of tweets to include
        :para maxId (optional) - maximum tweet id of tweets to include
        
    Return:
        Nothing
        
    """
    tweetCounter = 0
    #fid = open(outputFileName, 'wb')
    with codecs.open(outputFileName, 'wb', 'utf-8') as fid:
        for (index, filename) in enumerate(filenameList):
            print "{}, {}".format(index, filename)
            tweetList = ReadDumpFiles(filename, combine = True)
            for (tweetIndex, tweet) in enumerate(tweetList):
                if (('text' in tweet.keys()) and (tweetList[0]['id'] >= minId) and (tweetList[0]['id'] <= maxId)):
                    fid.write(tweet['text'] + ' ')
                    tweetCounter += 1