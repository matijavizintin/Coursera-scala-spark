package week4

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.util.Random

/**
  * Created by matijav on 20/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class DataSuite extends FunSuite {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    val sc = new SparkContext(conf)

    private val elements = 100 * 1000

    private val countries = List("Switzerland", "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "UK", "Portugal")
    private val genders = List("Male", "Female")

    private val demographicsList = prepareDemographic()
    private val financesList = prepareFinances()

    private def prepareDemographic() = {
        val random = Random

        (0 until elements).map(i => Demographic(
            i,
            random.nextInt(20) + 20,
            random.nextBoolean(),
            countries(random.nextInt(9)),
            genders(random.nextInt(1)),
            random.nextBoolean(),
            random.nextBoolean()
        ))
    }

    private def prepareFinances() = {
        val random = Random

        (0 until elements).map(i => Finances(
            i,
            random.nextBoolean(),
            random.nextBoolean(),
            random.nextBoolean(),
            random.nextInt(50000) + 10000
        ))
    }

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
