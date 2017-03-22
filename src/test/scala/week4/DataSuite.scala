package week4

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import week4.helpers.Generator._
import week4.helpers.Sparky

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataSuite extends Sparky {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    var sc: SparkContext = new SparkContext(conf)

    private val elements = 100 * 1000

    private val demographicsList = generateDemographic(elements)
    private val financesList = generateFinances(elements)

    test("test 1") {
        val demographics = sc.parallelize(demographicsList).map(x => (x.id, x))
        val finances = sc.parallelize(financesList).map(x => (x.id, x))

        val eligible = demographics.join(finances).filter {
            case (_, (d, f)) => d.country == "Switzerland" && f.hasDebt && f.hasFinancialDependents
        }.count

        println(eligible)
    }

    test("test 2") {
        val demographics = sc.parallelize(demographicsList).map(x => (x.id, x))
        val finances = sc.parallelize(financesList).map(x => (x.id, x))

        val eligible = demographics.filter {
            case (_, d) => d.country == "Switzerland"
        }.join(finances.filter {
            case (_, f) => f.hasDebt && f.hasFinancialDependents
        }).count

        println(eligible)
    }

    test("test 3") {
        val demographics = sc.parallelize(demographicsList).map(x => (x.id, x))
        val finances = sc.parallelize(financesList).map(x => (x.id, x))

        val eligible = demographics.cartesian(finances).filter{
            case ((id1, _), (id2, _)) => id1 == id2
        }.filter{
            case ((_, d), (_, f)) => d.country == "Switzerland" && f.hasFinancialDependents && f.hasDebt
        }.count

        println(eligible)
    }
}
