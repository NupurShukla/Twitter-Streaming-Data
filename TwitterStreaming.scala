import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.collection.mutable.Map

object TwitterStreaming {

  var N = 0
  val S = 100
  var sampleTweets = new ListBuffer[Status]()
  var sampleTweetLength = 0
  var hashtagsDict = Map[String, Int]()

  def ProcessTweetBatches(tweetBatches : RDD[Status]) : Unit = {

    val tweets = tweetBatches.collect()
    for(status <- tweets){

      if(N < S){
        sampleTweets.append(status)
        sampleTweetLength = sampleTweetLength + status.getText().length

        val hashTags = status.getHashtagEntities().map(_.getText)
        for(tag <- hashTags){
          if(hashtagsDict.contains(tag)){
            hashtagsDict(tag) += 1
          }
          else{
            hashtagsDict(tag) = 1
          }
        }
      }

      else{
        val j = Random.nextInt(N)
        if(j < S){
          val tweetToBeRemoved = sampleTweets(j)
          sampleTweets(j) = status
          sampleTweetLength = sampleTweetLength + status.getText().length - tweetToBeRemoved.getText().length

          // Remove old hashtags
          val hashTags = tweetToBeRemoved.getHashtagEntities().map(_.getText)
          for(tag <- hashTags){
            hashtagsDict(tag) -= 1
          }

          // Add new hashtags
          val newHashTags = status.getHashtagEntities().map(_.getText)
          for(tag <- newHashTags){
            if(hashtagsDict.contains(tag)){
              hashtagsDict(tag) += 1
            }
            else{
              hashtagsDict(tag) = 1
            }
          }

          val topTags = hashtagsDict.toSeq.sortWith(_._2 > _._2)
          val size = topTags.size.min(5)

          println("The number of the twitter from beginning: " + (N + 1))
          println("Top 5 hot hashtags:")

          for(i <- 0 until size){
            if(topTags(i)._2 != 0){
              println(topTags(i)._1 + ":" + topTags(i)._2)
            }
          }

          println("The average length of the twitter is: " +  sampleTweetLength/(S.toFloat))
          println("\n\n")
        }
      }
      N = N + 1
    }
  }

  def main(args: Array[String]): Unit = {

    val spark = new SparkConf()
    spark.setAppName("TwitterStreaming")
    spark.setMaster("local[*]")
    val sc = new SparkContext(spark)
    sc.setLogLevel(logLevel = "OFF")

    val consumer_key = "<ENTER_YOUR_OWN>"
    val consumer_secret = "<ENTER_YOUR_OWN>"
    val access_token = "<ENTER_YOUR_OWN>"
    val access_token_secret = "<ENTER_YOUR_OWN>"
    System.setProperty("twitter4j.oauth.consumerKey", consumer_key)
    System.setProperty("twitter4j.oauth.consumerSecret", consumer_secret)
    System.setProperty("twitter4j.oauth.accessToken", access_token)
    System.setProperty("twitter4j.oauth.accessTokenSecret", access_token_secret)

    val ssc = new StreamingContext(sc, Seconds(10))
    val tweets = TwitterUtils.createStream(ssc, None, Array("Data"))
    tweets.foreachRDD(tweetBatches => ProcessTweetBatches(tweetBatches))
    ssc.start()
    ssc.awaitTermination()
  }
}