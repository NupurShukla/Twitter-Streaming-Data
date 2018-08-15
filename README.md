# Twitter-Streaming-Data

This work was done as part of INF-553 (Foundations and Applications of Data Mining) coursework at USC.

I have used Twitter API of streaming to implement the fixed size sampling method (Reservoir Sampling Algorithm) and use the sampling to track the popular tags on tweets and calculate the average length of tweets. Implemented this in both Python and Scala.

<b>Environment requirements</b>-
Python 2.7, Spark 2.2.1 and Scala 2.11.8 

<b>NOTE</b>-
To run the code, you would need to create credentials for Twitter APIs in order to get tweets from Twitter. <br/>
For that register on https://apps.twitter.com/, click on "Create new app" and then fill the form click on "Create your
Twitter app." Second, go to the newly created app and open the "Keys and Access Tokens" tab. Then click on "Generate my access token." You will need to set these tokens as arguments in the code (Line 75-78)

I have used keyword “Data” to filter the tweets
Command Python: $SPARK_HOME/bin/spark-submit TwitterStreaming.py

