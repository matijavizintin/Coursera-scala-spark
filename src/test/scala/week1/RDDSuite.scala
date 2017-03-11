package week1

import org.apache.spark.util.Vector
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import week1.Helper._

import scala.util.Random

/**
  * Created by matijav on 11/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class RDDSuite extends FunSuite with Serializable {
    val path = "src/test/resources/"

    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local")
    val sc = new SparkContext(conf)

    test("Test spark simple") {
        val distFile = sc.textFile(path + "enwiki-latest-abstract.xml.gz")
        val rowCount = distFile
                .map(line => 1)
                .reduce((acc, v) => acc + v)

        print(rowCount)
    }

    test("Test fake logistic regression") {
        val r = new Random()

        val d = 10
        val numIterations = 10

        val alpha = Vector.random(d)
        val parsePoints: String => Point = _ => Point(r.nextDouble(), r.nextDouble())

        val points = sc.textFile(path + "enwiki-latest-abstract.xml.gz").map(parsePoints).persist()
        var w = Vector.zeros(d)

        for (_ <- 1 to numIterations) {
            val gradient = points.map {
                p => (1 / (1 + exp(-p.y * p.x)) - 1) * p.y * p.y
            }.reduce(_ + _)
            w -= alpha * gradient
        }
    }
}
