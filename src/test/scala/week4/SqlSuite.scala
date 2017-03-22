package week4

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import week4.helpers.Generator._
import week4.helpers.Sparky

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class SqlSuite extends Sparky {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    var sc: SparkContext = new SparkContext(conf)

    private val people = generatePeople()
    private val peopleJson = generatePeopleJson()

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
        val rdd = sc.parallelize(generatePeople(100)).map(p => (p.id, p.name, p.age, p.city, p.country))

        val spark = SparkSession.builder().appName("example").getOrCreate()
        import spark.implicits._
        val df = rdd.toDF("id", "name", "age", "city", "country")

        df.show()
        df.printSchema()
    }
}
