import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object PaloAltoRatingSQL {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[4]").appName("PaloAltoRatingSQL").getOrCreate()

    val businessrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/business.csv")
    val reviewrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/review.csv")

    val businessrow = businessrdd.map(line => line.split("::")).filter(x => x(1).contains("Palo Alto"))
                .map(x => (x(0), x(1), x(2))).map(a => Row(a._1, a._2, a._3))

    val ratingrow = reviewrdd.map(line => line.split("::"))
                    .map(x => (x(0), x(1), x(2), x(3)))
                    .map(a => Row(a._1, a._2, a._3, a._4.toDouble))

    import spark.implicits._

    val businessTable = new StructType()
                          .add(StructField("bid", StringType, true))
                          .add(StructField("addr", StringType, true))
                          .add(StructField("cat", StringType, true))

    val ratingTable = new StructType()
                          .add(StructField("rid", StringType, true))
                          .add(StructField("uid", StringType, true))
                          .add(StructField("bid", StringType, true))
                          .add(StructField("ratings", DoubleType))

    val businessdf = spark.createDataFrame(businessrow, businessTable)
    val ratingdf = spark.createDataFrame(ratingrow, ratingTable)

    businessdf.createOrReplaceTempView("BusinessView")
    ratingdf.createOrReplaceTempView("ReviewView")

    val BusiAvRatingJoin = spark.sql("SELECT R.uid as user_id,AVG(R.ratings) as avg_rating FROM BusinessView B INNER JOIN ReviewView R " +
      "ON B.bid = R.bid GROUP BY user_id ORDER BY avg_rating Desc")

    val BusiAvgRatingJoinrdd = BusiAvRatingJoin.rdd.map(x => x(0) + "\t" + x(1))

    BusiAvgRatingJoinrdd.collect().foreach(println)

  }
}
