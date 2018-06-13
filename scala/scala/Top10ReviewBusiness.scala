
import org.apache.spark._


object Top10ReviewBusiness {

  def parseLineReview(line: Array[String]) = {

    val rid = line(0)
    val uid = line(1)
    val bid = line(2)
    val rating = line(3)

    (bid, (rid, uid, rating))

  }

  def couting(tuples: Iterable[((String, String), (String, String, String))]) = {

    tuples.size / 2

  }

  def parseLine(line: Array[String]) = {

    val bid = line(0)
    val addr = line(1)
    val cat = line(2)

    (bid, (addr, cat))


  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Top10ReviewBusiness").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val businessrdd = sc.textFile("hdfs://localhost:9000/user/shalin/business.csv")
    val reviewrdd = sc.textFile("hdfs://localhost:9000/user/shalin/review.csv")

    val businesssplit = businessrdd.map(line => line.split("::"))
    val reviewsplit = reviewrdd.map(line => line.split("::"))
    val rev = reviewsplit.filter(x => x.size == 4)
    val businessID = businesssplit.filter(x => x.size == 3)
    val parsebusiness = businessID.map(parseLine)
    val parsereview = rev.map(parseLineReview)
    val joinedtable = parsebusiness.join(parsereview)
    val jionedtable1 = joinedtable.groupByKey()
    val count = jionedtable1.map(x => (x._1, couting(x._2).toInt))
    val joinTable2 = count.join(parsebusiness).distinct()
    val finaloutput = joinTable2.map(x => (x._1, x._2._2._1, x._2._2._2, x._2._1))
    val output = finaloutput.sortBy(x => x._4, false).take(10)

    output.foreach(println)

  }

}
