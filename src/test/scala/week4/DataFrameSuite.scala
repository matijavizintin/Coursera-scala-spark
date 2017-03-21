package week4

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataFrameSuite extends FunSuite {
    // disable spark logs
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[6]").set("spark.driver.memory", "4g")
    val sc = new SparkContext(conf)

    private val cities = List("Sydney", "Melbourne", "Perth", "Canberra", "Adelaide", "Darwin")
    private val states = List("A", "B", "C")

    private def generateEmployees(size: Int = 100) = {
        val rand = Random
        (0 until size).map(i => Employee(
            i,
            rand.nextString(6),
            rand.nextString(6),
            rand.nextInt(10) + 20,
            cities(rand.nextInt(6)),
            states(rand.nextInt(3))
        ))
    }
    private val employees = generateEmployees()
    private val rdd = sc.parallelize(employees).map(e => (e.id, e.fname, e.lname, e.age, e.city, e.state))

    private val spark = SparkSession.builder().appName("example").getOrCreate()
    import spark.implicits._

    private val dataFrame = rdd.toDF("id", "fname", "lname", "age", "city", "state")

    test("dataframe sql") {
        dataFrame.createOrReplaceTempView("employee")

        spark.sql("SELECT id, lname FROM employee WHERE city = 'Sydney' ORDER BY id").show()

        dataFrame.select("id", "lname").where("city == 'Sydney'").orderBy("id").show()
    }

    test("columns") {
        println(dataFrame.filter($"age" > 18).count())

        println(dataFrame.filter(dataFrame("age") > 18).count())

        println(dataFrame.filter("age > 18").count())
    }

    test("group by") {
        dataFrame.groupBy($"city").count().show()
    }

    test("aggregate") {
        val summed = dataFrame.groupBy($"city").agg(functions.sum("age"))
        val min = dataFrame.groupBy($"city").agg(functions.min("age"))
        val max = dataFrame.groupBy($"city").agg(functions.min("age"))

        min.show()
        summed.show()
        max.show()
    }

    test("aggregate2") {
        val ranked = dataFrame.groupBy($"age", $"state").agg(functions.count($"id")).orderBy($"state", $"count(id)".desc)
        ranked.show(100)
    }
}
