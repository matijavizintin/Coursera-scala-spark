package week1

import helpers.Common._
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

    test("Test spark simple") {
        val distFile = sc.textFile(PATH_TO_RESOURCES + "enwiki-latest-abstract.xml.gz")
        val rowCount = distFile
                .map(line => 1)
                .reduce((acc, v) => acc + v)

        print(rowCount)
    }

    test("Test fake logistic regression") {
        val r = new Random()

        val numIterations = 3

        val alpha = r.nextDouble()
        val parsePoints: String => Point = _ => Point(r.nextDouble(), r.nextDouble())

        val points = sc.textFile(PATH_TO_RESOURCES + "enwiki-latest-abstract.xml.gz").map(parsePoints).persist()
        var w = r.nextDouble()

        for (_ <- 1 to numIterations) {
            val gradient = points.map {
                p => (1 / (1 + exp(-p.y * p.x)) - 1) * p.y * p.y
            }.reduce(_ + _)
            w -= alpha * gradient
        }
    }
}
