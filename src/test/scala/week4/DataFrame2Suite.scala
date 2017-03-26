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
class DataFrame2Suite extends FunSuite {

    import spark.implicits._

    private val dfEmployees = sc.parallelize(generateEmployees(100))
            .map(e => (e.id, e.fname, e.lname, e.age, e.city, e.state))
            .toDF("id", "fname", "lname", "age", "city", "state")

    private val dfFinances = sc.parallelize(generateFinances(50))
            .map(f => (f.id, f.hasDebt, f.hasFinancialDependents, f.hasStudentLoans, f.income))
            .toDF("id", "debt", "financialdependents", "studentloan", "income")

    private val dfDemographic = sc.parallelize(generateDemographic(50))
            .map(d => (d.id, d.age, d.codingCamp, d.country, d.gender, d.isEthicalMinority, d.servedInMilitary))
            .toDF("id", "age", "codingcamp", "country", "gender", "ethnicalminority", "servedinmilitary")

    test("cleaning dataframes") {
        val data = List(
            (1, "name1", "12"),
            (2, null, "13"),
            (3, "name3", null)
        )

        val dataFrame = sc.parallelize(data).toDF("id", "name", "age")
        dataFrame.na.drop().show()
    }

    test("joins") {
        dfEmployees.join(dfFinances, dfEmployees("id") === dfFinances("id")).orderBy(dfEmployees("id").desc).show()

        dfEmployees.join(dfFinances, dfEmployees("id") === dfFinances("id"), "left_outer").orderBy(dfEmployees("id").desc_nulls_first).show()
    }

    test("joins 2") {
        val cnt = dfDemographic.join(dfFinances, dfDemographic("id") === dfFinances("id"))
                .filter($"debt" && $"financialdependents")
                .filter($"country" === "Switzerland").count
        println(cnt)
    }
}
