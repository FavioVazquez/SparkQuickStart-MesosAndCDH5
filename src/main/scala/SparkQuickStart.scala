import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import com.databricks.spark.csv._
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * Created by Favio on 14/05/15.
 */
object SparkQuickStart {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf()
      //      .setMaster("local")
      .setMaster("mesos://master.mcbo.mood.com.ve:5050")
      .setAppName("Jaccard Similarity")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    sc.stop()
  }
}
