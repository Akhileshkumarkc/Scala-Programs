
import org.apache.spark._

import scala.collection.SortedMap

object Top10FriendsPair {

  def parseRecord(record: Array[String]) = {

    val key = record(0)
    val value = record(1).split(",")
    val friendPairs = value.map(friend => {

      if (key < friend) {
        (key, friend)
      }
      else {
        (friend, key)
      }
    })

    friendPairs.map(pair => (pair, value.toSet))

  }

  def parseReducer(friendList1: Set[String], friendList2: Set[String]) = {

    friendList1.intersect(friendList2)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("Top10FriendsPair")
    val sc = new SparkContext(conf)

    val friendsrdd = sc.textFile("hdfs://localhost:9000/user/shalin/soc-LiveJournal1Adj.txt")
    val userdata = sc.textFile("hdfs://localhost:9000/user/shalin/userdata.txt")

    val splitrdd = friendsrdd.map(record => record.split("\\t"))
    val filteredrdd = splitrdd.filter(record => (record.size == 2))
    val userdatardd = userdata.map(x => x.split(","))
    val parsedUserData = userdatardd.map(line => (line(0), (line(1), line(2), line(3), line(4), line(5), line(6), line(7),
      line(8), line(9))))
    val filteredMap = filteredrdd.flatMap(parseRecord)
    val keyMap = filteredMap.reduceByKey(parseReducer).filter(!_._2.isEmpty).sortByKey()

    val results = keyMap.map(x => (x._1, x._2.size.toInt))
    val sorteddata = results.sortBy(x => x._2, false).take(10)

    val sortedrdd_1 = sc.parallelize(sorteddata).map(x => (x._1._1, (x._1._2, x._2))).join(parsedUserData).map(x =>
      (x._2._1._1, (x._1, x._2._1._2, x._2._2))).join(parsedUserData)

    val finalresult = sortedrdd_1.map(x =>
      (x._2._1._2 + "\t" + x._2._1._3._1 + "\t" + x._2._1._3._2 + "\t" + x._2._1._3._3 + "\t" + x._2._1._3._4 +
        "\t" + x._2._1._3._5 + "\t" + x._2._1._3._6 + "\t" + x._2._1._3._7 + "\t" + x._2._1._3._8 + "\t" + x._2._1._3._9
      + "\t" + x._2._2._1 + "\t" + x._2._2._2 + "\t" + x._2._2._3 + "\t" + x._2._2._4 + "\t" + x._2._2._5
        + "\t" + x._2._2._6 + "\t" + x._2._2._7  + "\t" + x._2._2._8 + "\t" + x._2._2._9))

    finalresult.collect().foreach(println)

  }
}
