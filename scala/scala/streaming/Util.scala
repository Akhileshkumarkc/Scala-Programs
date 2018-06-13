
import twitter4j._

object Util {
  val config = new twitter4j.conf.ConfigurationBuilder()
  .setOAuthConsumerKey("jrPlepQGYmtZkO4locnUwawHe")
  .setOAuthConsumerSecret("JWREIWrENWcGt37FEyTPhfE34j4O1w6kkF02wCUhLB28blZ0nq")
  .setOAuthAccessToken("899279922639675392-qWkTEtTiWJ6dYPrefliL21s2FkqWY6I")
  .setOAuthAccessTokenSecret("K6rXIUlCMl7HUFYPIXvbgC14DY4LrLtgxjIbGh5aavZWN").build()

  def simpleStatusListener = new StatusListener() {
    def onStatus(status: Status) { println(status.getText) }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
    def onException(ex: Exception) { ex.printStackTrace }
    def onScrubGeo(arg0: Long, arg1: Long) {}
    def onStallWarning(warning: StallWarning) {}
  }
 
  }
