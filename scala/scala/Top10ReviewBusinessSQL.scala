
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Top10ReviewBusinessSQL {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[4]").appName("Q3sql").getOrCreate()

    val businessrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/business.csv")
    val reviewrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/review.csv")

    val businessRow = businessrdd.map(line => line.split("::")).map(x => (x(0), x(1), x(2))).map(a => Row(a._1, a._2, a._3))

    val reviewRow = reviewrdd.map(line => line.split("::")).map(x => (x(0), x(1), x(2), x(3))).map(a => Row(a._1, a._2, a._3, a._4.toDouble))

    import spark.implicits._

    val businessTable = new StructType()
                      .add(StructField("bid", StringType, true))
                      .add(StructField("addr", StringType, true))
                     .add(StructField("cat", StringType, true))

    val reviewTable = new StructType()
                    .add(StructField("rid", StringType, true))
                    .add(StructField("uid", StringType, true))
                  .add(StructField("bid", StringType, true))
                    .add(StructField("ratings", DoubleType))

    val businessdf = spark.createDataFrame(businessRow, businessTable)
    val reviewdf = spark.createDataFrame(reviewRow, reviewTable)

    businessdf.createOrReplaceTempView("Business")
    reviewdf.createOrReplaceTempView("Review")

    val joining = spark.sql("SELECT b.bid as business_id,b.addr as full_address,b.cat as categories,COUNT(r.ratings)/2 as number_rated FROM Business b INNER JOIN Review r " +
      "ON b.bid = r.bid GROUP BY business_id,full_address,categories ORDER BY number_rated Desc LIMIT 10")

    val Resultrdd = joining.rdd.map(x => x(0) + "\t" + x(1) + "\t" + x(2) + "\t" + x(3))

    Resultrdd.collect().foreach(println)

  }
}