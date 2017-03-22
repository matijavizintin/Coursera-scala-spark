package week4.helpers

import week4.{Demographic, Employee, Finances, Person}

import scala.util.Random
import scala.util.parsing.json.JSONObject

/**
  * Created by matijav on 22/03/2017.
  */
object Generator {
    private val cities = List("Sydney", "Melbourne", "Perth", "Canberra", "Adelaide", "Darwin")
    private val states = List("A", "B", "C")
    private val countries = List("Switzerland", "Germany", "France", "Italy", "Spain", "Netherlands", "Belgium", "UK", "Portugal")
    private val genders = List("Male", "Female")

    def generateEmployees(size: Int = 100) = {
        val rand = Random
        (0 until size).map(i => Employee(
            i,
            rand.nextString(6),
            rand.nextString(6),
            rand.nextInt(10) + 20,
            cities(rand.nextInt(6)),
            states(rand.nextInt(3))
        ))
    }

    def generateFinances(size: Int = 50) = {
        val random = Random

        (0 until size).map(i => Finances(
            i,
            random.nextBoolean(),
            random.nextBoolean(),
            random.nextBoolean(),
            random.nextInt(50000) + 10000
        ))
    }

    def generateDemographic(size: Int = 50) = {
        val random = Random

        (0 until size).map(i => Demographic(
            i,
            random.nextInt(20) + 20,
            random.nextBoolean(),
            countries(random.nextInt(9)),
            genders(random.nextInt(1)),
            random.nextBoolean(),
            random.nextBoolean()
        ))
    }

    def generatePeople(size: Int = 100000) = {
        val rand = Random
        (0 until size).map(i => Person(i, rand.nextString(6), rand.nextInt(60), rand.nextString(2), rand.nextString(2)))
    }

    def generatePeopleJson(size: Int = 100) = {
        generatePeople(size).map(p => Map(
            ("id", p.id),
            ("name", p.name),
            ("age", p.age),
            ("city", p.city),
            ("country", p.country)
        )).map(JSONObject).map(_.toString())
    }
}
