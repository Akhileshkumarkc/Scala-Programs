
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}

object MutualFriendsSQL {


  def parseRecord(line: Array[String]) = {

    val key = line(0)
    val word = line(1).split(",")
    val friendPairs = word.map(friend => {

      if (key < friend) {
        (key, friend)
      }
      else {
        (friend, key)
      }
    })
    friendPairs.map(pair => (pair, word.toSet))

  }

  def parseReducer(friends1: Set[String], friends2: Set[String]) = {

    friends1.intersect(friends2)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[4]").appName("MutualFriendsSQL").getOrCreate()

    val inputrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/soc-LiveJournal1Adj.txt")

    val splitrdd = inputrdd.map(line => line.split("\\t"))
    val filteredrdd = splitrdd.filter(record => (record.size == 2))

    val parsedRecord = filteredrdd.flatMap(parseRecord)
    val result = parsedRecord.reduceByKey(parseReducer)

    val rowrdd = result.map(x => (x._1._1, x._1._2, x._2.toList)).map(attrs => Row(attrs._1, attrs._2, attrs._3))
    val Table = new StructType().add(StructField("friend1", StringType, true)).add(StructField("friend2", StringType, true))
      .add(StructField("Mutual_Friend_List", ArrayType(StringType), true))

    val df = spark.createDataFrame(rowrdd, Table)

    df.createOrReplaceTempView("MutualFriends")

    val results = spark.sql("select friend1,friend2,size(Mutual_Friend_List) as count from MutualFriends ORDER BY count DESC")
    val resultrdd = results.rdd.map(x => x(0) + "," + x(1) + "\t" + x(2))

    resultrdd.collect().foreach(println)

  }

}
