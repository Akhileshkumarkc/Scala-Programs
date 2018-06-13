import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.mllib.recommendation.ALS

object CollaborativeFiltering {
  def main(args: Array[String]): Unit = {
    
    val configuration = new SparkConf().setAppName("CollaborativeFiltering").setMaster("local")
  
    val sc = new SparkContext(configuration)
    
    val spark = SparkSession.builder.getOrCreate()
    
    val data = sc.textFile("ratings.dat").map(line => line.split("::") match { case Array(u_id, m_id, rating, timestamp) =>
      Rating(u_id.toInt, m_id.toInt, rating.toDouble)
    })
    
    var splits = data.randomSplit(Array(6.0,4.0),24)
    val training = splits(0);
    val test = splits(1);
    
    val rank = 10
    val numIterations = 10
    val model = ALS.train(training, rank, numIterations, 0.06)
    
    // Evaluate the model on rating data
    val usersProducts = test.map { case Rating(u_id, m_id, rating) =>
      (u_id, m_id)
    }
    val predictions = model.predict(usersProducts).map { case Rating(u_id, m_id, rating) =>
        ((u_id, m_id), rating)
    }
    
    val combined_result = test.map { case Rating(u_id, m_id, rating) =>
      ((u_id, m_id), rating)
    }.join(predictions)
    
    val MSE = combined_result.map { case ((u_id, m_id), (r1, r2)) =>
      val err = (r1 - r2)
      err * err
    }.mean()
    println("Mean Squared Error = " + MSE)

    sc.stop()
  }
}