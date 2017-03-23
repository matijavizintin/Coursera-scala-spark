package week4

import helpers.Common._
import helpers.Generator._
import org.apache.spark.sql.functions
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataFrameSuite extends FunSuite {
    import spark.implicits._

    private val rdd = sc.parallelize(generateEmployees(100)).map(e => (e.id, e.fname, e.lname, e.age, e.city, e.state))
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
