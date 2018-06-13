
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark._
import org.apache.spark.sql.types._

object Top10FriendsPairSQL {

  def parseRecord(record: Array[String]) = {

    val key = record(0)
    val word = record(1).split(",")
    val Friendpairs = word.map(friend => {

      if (key < friend) {
        (key, friend)
      }
      else {
        (friend, key)
      }
    })
    Friendpairs.map(pair => (pair, word.toSet))

  }

  def parseReducer(friendList1: Set[String], friendList2: Set[String]) = {

    friendList1.intersect(friendList2)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.master("local[4]").appName("Top10FriendsPairSQL").getOrCreate()


    val inputrdd = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/soc-LiveJournal1Adj.txt")

    val userdata = spark.sparkContext.textFile("hdfs://localhost:9000/user/shalin/userdata.txt")

    val userrow = userdata.map(x => x.split(",")).map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9)))
      .map(attr => Row(attr._1, attr._2, attr._3, attr._4, attr._5, attr._6, attr._7, attr._8, attr._9, attr._10))

    val userTable = new StructType().add(StructField("uid", StringType, true)).add(StructField("fname", StringType, true))
      .add(StructField("lname", StringType, true)).add(StructField("addr", StringType, true)).add(StructField("city", StringType, true))
      .add(StructField("state", StringType, true)).add(StructField("zip", StringType, true)).add(StructField("country", StringType, true))
      .add(StructField("uname", StringType, true)).add(StructField("dob", StringType, true))

    val userdf = spark.createDataFrame(userrow, userTable)
    val splitrdd = inputrdd.map(line => line.split("\\t"))
    val filteredRdd = splitrdd.filter(line => (line.size == 2))
    val parseRdd = filteredRdd.flatMap(parseRecord)
    val reduceRdd = parseRdd.reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()
    val irdd = reduceRdd.map(x => (x._1._1, x._1._2, x._2.toList.size)).sortBy(x => x._3, false).take(10)

    val toptenrdd = spark.sparkContext.parallelize(irdd)

    val rowrdd = toptenrdd.map(attrs => Row(attrs._1, attrs._2, attrs._3))

    val table = new StructType()
            .add(StructField("uid", StringType, true))
            .add(StructField("friend2", StringType, true))
            .add(StructField("Mutual_List_Count", IntegerType, true))

    val inputdf = spark.createDataFrame(rowrdd, table)
    inputdf.createOrReplaceTempView("FriendsTable")
    userdf.createOrReplaceTempView("UserTable")

    val intresult = spark.sql("SELECT i.friend2 as uid,i.Mutual_List_Count,u.fname,u.lname,u.addr,u.city,u.state" +
      ",u.zip,u.country,u.uname,u.dob FROM UserTable u INNER JOIN FriendsTable i ON u.uid = i.uid")
    intresult.createOrReplaceTempView("inttable")
    val result = spark.sql("SELECT i.Mutual_List_Count,i.fname,i.lname,i.addr,i.city,i.state,i.zip,i.country" +
      ",i.uname,i.dob,u.fname as fname2,u.lname as lname2,u.addr as addr2,u.city as city2,u.state as state2," +
      "u.zip as zip2,u.country as country2,u.uname as uname2,u.dob as dob2 FROM UserTable u INNER JOIN inttable i " +
      "ON u.uid = i.uid")

    val resultrdd = result.rdd.map(x => x(0) + "\t" + x(1) + "\t" + x(2) + "\t" + x(3) + "\t" + x(4) + "\t" + x(5) + "\t" + x(6) + "\t" + x(7)
      + "\t" + x(8) + "\t" + x(9) + "\t" + x(10) + "\t" + x(11) + "\t" + x(12) + "\t" + x(13) + "\t" + x(14) + "\t" + x(15) + "\t" + x(16) + "\t" + x(17) + "\t"
      + x(18))

    resultrdd.collect().foreach(println)

  }

}
