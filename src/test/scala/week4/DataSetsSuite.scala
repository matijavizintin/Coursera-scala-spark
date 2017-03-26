package week4

import helpers.Common._
import helpers.Generator._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by matijav on 22/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataSetsSuite extends FunSuite {

    import spark.implicits._

    private val employees = generateEmployees(100)

    private val rdd = sc.parallelize(employees)
    private val rddTuple = sc.parallelize(employees).map(e => (e.id, e.fname, e.lname, e.age, e.city, e.state))

    private val employeesDataSet = rdd.toDS()

    private val tupleDataSet = rddTuple.toDS()
    private val tupleDataFrame = rddTuple.toDF("id", "fname", "lname", "age", "city", "state")

    test("mixing style") {
        // .as[Type] creates a TypedColumn
        val summed = employeesDataSet.groupByKey(e => e.state).agg(functions.sum("age").as[Double])

        summed.show()
    }

    test("create dataset") {
        val peopleJson = generatePeopleJson(15)
        val jsonRDD = sc.parallelize(peopleJson)

        val spark = SparkSession.builder().appName("example").getOrCreate()
        val dataSet = spark.read.json(jsonRDD).as[Person]

        dataSet.show()
    }

    test("map dataset") {
        val peopleJson = generatePeopleJson(15)
        val jsonRDD = sc.parallelize(peopleJson)

        val spark = SparkSession.builder().appName("example").getOrCreate()
        val dataSet = spark.read.json(jsonRDD).as[Person]

        dataSet.map(p => p.age).distinct().show()
    }

    test("groupBy dataset") {
        val peopleJson = generatePeopleJson(15)
        val jsonRDD = sc.parallelize(peopleJson)

        val spark = SparkSession.builder().appName("example").getOrCreate()
        val dataSet = spark.read.json(jsonRDD).as[Person]

        // avg
        println("Average")
        dataSet.groupByKey(p => p.age).agg(functions.avg($"age").as[Double]).show()

        // map groups
        println("Map groups")
        dataSet.groupByKey(p => p.age).mapGroups {
            case (k, iter) => k
        }.show()

        // flat map groups
        println("Flat map groups")
        dataSet.groupByKey(p => p.age).flatMapGroups {
            case (k, iter) => iter
        }.show()

        // reduce groups
        println("Reduce groups")
        dataSet.groupByKey(p => p.age).reduceGroups((p1, _) => p1).show()
    }

    test("reduce by key") {
        val keyValues = List((1, "A"), (2, "B"))
        val keyValuesDS = keyValues.toDS()

        keyValuesDS.groupByKey {
            case (k, _) => k
        }.mapGroups( // mapGroups performs shuffling, expensive
            (k, vs) =>
                (k, vs.foldLeft("") {
                    case (acc, (_, v)) => acc + v
                })
        ).show()
    }

    test("reduce by key 2") {
        val keyValues = List((1, "A"), (2, "B"))
        val keyValuesDS = keyValues.toDS()

        keyValuesDS.groupByKey {
            case (k, _) => k
        }.mapValues {
            case (_, vs) => vs
        }.reduceGroups((acc, str) => acc + str).show()
    }

    test("reduce by key 3") {
        val keyValues = List((1, "A"), (2, "B"))
        val keyValuesDS = keyValues.toDS()

        val aggr = new Aggregator[(Int, String), String, String] {
            def zero: String = ""

            def reduce(b: String, a: (Int, String)): String = a match {
                case (_, y) => b + y
            }

            def merge(b1: String, b2: String): String = b1 + b2

            def finish(reduction: String): String = reduction

            override def bufferEncoder: Encoder[String] = Encoders.STRING // encoder to encode data for Tungsten

            override def outputEncoder: Encoder[String] = Encoders.STRING
        }.toColumn

        keyValuesDS.groupByKey {
            case (k, _) => k
        }.agg(aggr.as[String]).show()
    }
}