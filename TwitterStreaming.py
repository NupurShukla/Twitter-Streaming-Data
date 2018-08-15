import tweepy
import random
import operator

N = 0
S = 100
sampleTweets = []
sampleTweetLength = 0
hashtagsDict = {}

class MyStreamListener(tweepy.StreamListener):
	
	def on_status(self, status):
		global N
		global S
		global sampleTweets
		global sampleTweetLength
		global hashtagsDict
		
		if N < S:
			sampleTweets.append(status)
			sampleTweetLength += len(status.text)

			hashtags = status.entities['hashtags']
			for hashtag in hashtags:
				tag = hashtag['text']
				if tag in hashtagsDict:
					hashtagsDict[tag] += 1
				else:
					hashtagsDict[tag] = 1


		else:
			j = random.randrange(0, N)
			if j < S:
				tweetToBeRemoved = sampleTweets[j]
				sampleTweets[j] = status 
				sampleTweetLength = sampleTweetLength + len(status.text) - len(tweetToBeRemoved.text)

				# Remove old hashtags
				hashtags = tweetToBeRemoved.entities['hashtags']
				for hashtag in hashtags:
					tag = hashtag['text']
					hashtagsDict[tag] -= 1

				# Add new hashtags
				hashtags = status.entities['hashtags']
				for hashtag in hashtags:
					tag = hashtag['text']
					if tag in hashtagsDict:
						hashtagsDict[tag] += 1
					else:
						hashtagsDict[tag] = 1

				# Top 5 hashtags
				topTags = sorted(hashtagsDict.items(), key=operator.itemgetter(1), reverse=True)[:5]

				print "The number of the twitter from beginning:", N + 1
				print "The top 5 hot hashtags:"
				for topTag in topTags:
					if topTag[1]==0:
						continue
					print topTag[0].encode('utf-8') + ":" + str(topTag[1])

				print "The average length of the twitter is:", sampleTweetLength/float(S)
				print "\n\n"

		N = N + 1

	def on_error(self, status_code):
		if status_code == 420:
			#returning False in on_data disconnects the stream
			return False

consumer_key = "<ENTER_YOUR_OWN>"
consumer_secret = "<ENTER_YOUR_OWN>"
access_token = "<ENTER_YOUR_OWN>"
access_token_secret = "<ENTER_YOUR_OWN>"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

#myStream.sample(languages=['en'])
myStream.filter(track=['Data'])