package week4

import helpers.Common._
import helpers.Generator._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataSuite extends FunSuite {
    private val elements = 100 * 1000

    private val demographics = sc.parallelize(generateDemographic(elements)).map(x => (x.id, x))
    private val finances = sc.parallelize(generateFinances(elements)).map(x => (x.id, x))

    test("test 1") {
        val eligible = demographics.join(finances).filter {
            case (_, (d, f)) => d.country == "Switzerland" && f.hasDebt && f.hasFinancialDependents
        }.count

        println(eligible)
    }

    test("test 2") {
        val eligible = demographics.filter {
            case (_, d) => d.country == "Switzerland"
        }.join(finances.filter {
            case (_, f) => f.hasDebt && f.hasFinancialDependents
        }).count

        println(eligible)
    }

    test("test 3") {
        val eligible = demographics.cartesian(finances).filter{
            case ((id1, _), (id2, _)) => id1 == id2
        }.filter{
            case ((_, d), (_, f)) => d.country == "Switzerland" && f.hasFinancialDependents && f.hasDebt
        }.count

        println(eligible)
    }
}
