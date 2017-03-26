package week4

import helpers.Common._
import helpers.Generator._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class SqlSuite extends FunSuite {
    private val people = generatePeople(100)
    private val peopleJson = generatePeopleJson(100)

    test("create data frame from schema") {
        val schemaColumns = List("id", "name", "age", "city", "country")
        val schemaTypes = List(DataTypes.IntegerType, DataTypes.StringType, DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType)

        val fields = schemaColumns.zip(schemaTypes).map {
            case (col, dt) => StructField(col, dt, nullable = true)
        }
        val schema = StructType(fields)

        val rowRDD = sc.parallelize(people).map(p => Row(p.id, p.name, p.age, p.city, p.country))

        val spark = SparkSession.builder().appName("example").getOrCreate()
        val dataFrame = spark.createDataFrame(rowRDD, schema)

        dataFrame.createOrReplaceTempView("people")

        val adults = spark.sql("SELECT * FROM people WHERE age > 17 ORDER BY id").count()
        println(adults)
    }

    test("create data frame from json") {
        val jsonRDD = sc.parallelize(peopleJson)

        val spark = SparkSession.builder().appName("example").getOrCreate()
        val dataFrame = spark.read.json(jsonRDD)

        dataFrame.createOrReplaceTempView("people")

        val adults = spark.sql("SELECT * FROM people WHERE age > 17 ORDER BY id").count()
        println(adults)
    }

    test("create data frame with toDF") {
        val rdd = sc.parallelize(people).map(p => (p.id, p.name, p.age, p.city, p.country))

        val spark = SparkSession.builder().appName("example").getOrCreate()
        import spark.implicits._
        val dataFrame = rdd.toDF("id", "name", "age", "city", "country")

        dataFrame.createOrReplaceTempView("people")

        val adults = spark.sql("SELECT * FROM people WHERE age > 17 ORDER BY id").count()
        println(adults)
    }

    test("data frame show") {
        val rdd = sc.parallelize(people).map(p => (p.id, p.name, p.age, p.city, p.country))

        val spark = SparkSession.builder().appName("example").getOrCreate()
        import spark.implicits._
        val df = rdd.toDF("id", "name", "age", "city", "country")

        df.show()
        df.printSchema()
    }
}
