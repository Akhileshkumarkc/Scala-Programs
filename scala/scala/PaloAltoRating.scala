import org.apache.spark._

object PaloAltoRating {

  def parseRecordReview(record: Array[String]) = {

    val reviewid = record(0)
    val userid = record(1)
    val businessId = record(2)
    val stars = record(3)

    (businessId, (reviewid, userid, stars))

  }

  def parseRecord(record: Array[String]) = {

    val businessId = record(0)
    val address = record(1)
    val category = record(2)

    (businessId, (address, category))


  }

  def avgRating(tuple: (String, Iterable[(String, Float)])) = {

    val userId = tuple._1
    var sum, count, avg = 0.0
    val ratingArr = tuple._2.toArray
    count = ratingArr.size

    for (t <- ratingArr) {

      sum += t._2.toFloat

    }

    avg = sum / count

    (userId + "\t", avg)

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("PaloAltoRating").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val business_rdd = sc.textFile("hdfs://localhost:9000/user/shalin/business.csv")
    val review_rdd = sc.textFile("hdfs://localhost:9000/user/shalin/review.csv")

    val businesssplit = business_rdd.map(record => record.split("::"))
    val reviewsplit = review_rdd.map(record => record.split("::"))
    val locationCol = 1
    val reviews = reviewsplit.filter(x => x.size == 4)
    val businessId = businesssplit.filter(x => x.size == 3)
    val businessfilter = businessId.filter(line => line(locationCol).contains("Palo Alto"))

    val businessMap = businessfilter.map(parseRecord)
    val reviewMap = reviews.map(parseRecordReview)

    val busiReviewMap = businessMap.join(reviewMap).distinct()
    val busiReviewrdd = busiReviewMap.map(x => (x._2._2._2, (x._1, x._2._2._3.toFloat)))
    val busiReviewrddKey = busiReviewrdd.groupByKey()
    val busiReviewrddfinal = busiReviewrddKey.map(x => avgRating(x)).sortBy(x => x._2, false)

    busiReviewrddfinal.collect().foreach(println)


  }


}
