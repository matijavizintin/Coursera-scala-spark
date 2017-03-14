package week2

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
  * Created by matijav on 13/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class PairRDDSuite extends FunSuite {
    val path = "src/test/resources/"

    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    val sc = new SparkContext(conf)

    test("Test pair RDD") {
        val distFile = sc.textFile(path + "wikipedia.dat.gz").map(WikipediaArticle.parse)

        // create pair RDD
        val pairRdd = distFile.map(article => (article.title, article.text)).cache()

        println(pairRdd.take(10).map { case (k, v) => k + " --> " + v }.mkString("\n"))

        println(pairRdd.groupByKey().take(5).map { case (k, list) => k + " --> " + list.mkString("\n") }.mkString("\n"))
    }

    val events = List(
        Event("Org1", "Ev1", 100),
        Event("Org1", "Ev2", 50),
        Event("Org1", "Ev3", 80),
        Event("Org2", "Ev4", 30),
        Event("Org2", "Ev5", 77),
        Event("Org2", "Ev6", 88),
        Event("Org2", "E7", 46),
        Event("Org3", "Ev8", 12),
        Event("Org4", "Ev9", 45),
        Event("Org4", "Ev10", 98),
        Event("Org5", "Ev11", 115),
        Event("Org5", "Ev12", 216),
        Event("Org5", "Ev13", 41),
        Event("Org6", "Ev14", 22)
    )

    test("Group by key") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.budget))

        pairRdd.groupByKey().foreach(println)
    }

    test("Reduce by key") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.budget))

        // group by key + reduce
        pairRdd.reduceByKey((acc, b) => acc + b).foreach(println)
    }

    test("map values") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.budget))

        // short for map((k,v) => (k, f(v))
        pairRdd.mapValues(b => b * 2).foreach(println)
    }

    test("count by key") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.budget))

        // events per organizer
        println(pairRdd.countByKey())
    }

    test("average budget") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.budget))

        // avg budget per organizer
        pairRdd.mapValues(b => (b, 1)).reduceByKey {
            case ((b1, cnt1), (b2, cnt2)) => (b1 + b2, cnt1 + cnt2)
        }.mapValues {
            case (b, cnt) => b / cnt
        }.collect().foreach(println)
    }

    test("keys") {
        val distFile = sc.parallelize(events)

        // create pair RDD
        val pairRdd = distFile.map(e => (e.organizer, e.name)).cache()

        // distinct organizers
        println(pairRdd.keys.distinct().count())
    }

    val (card1, card2, card3) = (1, 2, 3)

    val as = List(
        (101, ("Loc1", card1)),
        (102, ("Loc2", card2)),
        (103, ("Loc3", card3)),
        (104, ("Loc4", card1))
    )

    val loc = List(
        (101, "LocX"),
        (101, "LocY"),
        (102, "LocZ"),
        (102, "LocW"),
        (102, "LocQ"),
        (103, "LocXY"),
        (103, "LocYZ"),
        (103, "LocWQ")
    )

    test("join") {
        val asRDD = sc.parallelize(as)
        val locRDD = sc.parallelize(loc)

        asRDD.join(locRDD).collect().foreach(println)
    }

    test("left/right outer join") {
        val asRDD = sc.parallelize(as)
        val locRDD = sc.parallelize(loc)

        // left join
        asRDD.leftOuterJoin(locRDD).collect().foreach(println)

        // right join
        asRDD.rightOuterJoin(locRDD).collect().foreach(println)
    }
}
