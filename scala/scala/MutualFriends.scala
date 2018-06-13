import org.apache.spark._

object MutualFriends {


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

    // send the common input of the set.
    friendList1.intersect(friendList2)

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[4]").setAppName("MutualFriends")
    val sc = new SparkContext(conf)

    val inputrdd = sc.textFile("hdfs://localhost:9000/user/shalin/soc-LiveJournal1Adj.txt")
    val splitrdd = inputrdd.map(record => record.split("\\t"))
    val filteredrdd = splitrdd.filter(record => (record.size == 2))

    val parsedRecord = filteredrdd.flatMap(parseRecord)
    val result = parsedRecord.reduceByKey(parseReducer)
    // check if the second tuple is empty.
    // sort by the Friend list
    val filteredResult = result.filter(!_._2.isEmpty).sortByKey()

    val finalResults = filteredResult.map(x => (x._1, x._2.size)).map(x => x._1.toString() + "\t" + x._2.toString())


    finalResults.collect().foreach(println)

  }

}