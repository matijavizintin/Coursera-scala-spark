package helpers

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by matijav on 22/03/2017.
  */
object Common {
    // disable spark logs
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    val sc: SparkContext = new SparkContext(conf)
    val spark = SparkSession.builder().appName("example").getOrCreate()

    val PATH_TO_RESOURCES = "../../src/test/resources/"
}
