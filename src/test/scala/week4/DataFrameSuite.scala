package week4

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import week4.helpers.Generator._
import week4.helpers.Sparky

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataFrameSuite extends Sparky {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    var sc: SparkContext = new SparkContext(conf)

    private val employees = generateEmployees(100)
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

        // debug info about result
        ranked.head.schema.printTreeString()
    }
}
