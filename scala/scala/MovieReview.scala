import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix

object MovieReview {
  def main(args: Array[String]): Unit = {
  
    val configuration = new SparkConf().setAppName("MovieReview").setMaster("local")
  
    val sc = new SparkContext(configuration)
        
    // Load and parse the data
    val itemFile = sc.textFile("itemusermat")
  
    val movieFile = sc.textFile("movies.dat")
    
    val parsedItemData = itemFile.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()
    
    val parsedMovieData = movieFile.map(s => s.split("::")).map(s => (s(0),(s(1),s(2))))
    
    val numClusters = 10
    val numIterations = 30
    
    val clusters = KMeans.train(parsedItemData, numClusters, numIterations)
    
    val moviesRating = itemFile.map(s => (s.split(' ')(0), Vectors.dense(s.split(' ').drop(1).map(_.toDouble))))
    
    val clusterRating = moviesRating.map(s => (s._1, clusters.predict(s._2)))
    
    val join = clusterRating.join(parsedMovieData)
    
    val result = join.map(x => (x._2._1,(x._1, x._2._2._1,x._2._2._2))).groupByKey().mapValues(_.take(5))
    
    
    result.map(x => (x._2.map(y => y._1+"\t"+y._2+"\t"+y._3))+ "\t"+ x._1).saveAsTextFile("movie")
  
  }
}