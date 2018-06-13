  import org.apache.spark.{SparkConf,SparkContext}
  import org.apache.spark.streaming.dstream.DStream
  import org.apache.spark.streaming.{Seconds,StreamingContext}
  import org.apache.spark.Logging
  import org.apache.spark.streaming.twitter.TwitterUtils
  import twitter4j._
  

  object TwitterStream {
    
    def locationCheck(tweet: Status) :Boolean = {
      if(tweet.getPlace()!=null){
        return true;
      }
      return false;
      
    }
    
    def getHashTagString(arr: Array[HashtagEntity]): String = {
      var res = ""
      arr.foreach(f =>res+="#"+f.getText()+"\t")
      return res
    }
    def main(args: Array[String]): Unit = {
      //setting up the configuration
      val conf = new SparkConf().setAppName("Twitterstream").setMaster("local[2]")
      val sc = new SparkContext(conf)
      val filters = Seq("Trump","Obama")
       
      sc.setLogLevel("OFF")
      //sparkstreaming context
      
      System.setProperty("twitter4j.oauth.consumerKey", "jrPlepQGYmtZkO4locnUwawHe")
      System.setProperty("twitter4j.oauth.consumerSecret","JWREIWrENWcGt37FEyTPhfE34j4O1w6kkF02wCUhLB28blZ0nq")
      System.setProperty("twitter4j.oauth.accessToken", "899279922639675392-qWkTEtTiWJ6dYPrefliL21s2FkqWY6I")
      System.setProperty("twitter4j.oauth.accessTokenSecret", "K6rXIUlCMl7HUFYPIXvbgC14DY4LrLtgxjIbGh5aavZWN")
        
      val ssc = new StreamingContext(sc, Seconds(2))     
      val stream = TwitterUtils.createStream(ssc, None,filters).filter(locationCheck)
      
      val tweets = stream.filter(t=>t.getText()!="").map(t=> println(getHashTagString(t.getHashtagEntities())+"\t"
          +SentimentAnalysis.detectSentiment(t.getText).toString+"\t\t"+t.getText+"\t"+t.getPlace().getName()))
      tweets.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
    


