package week3

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by matijav on 14/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class RDDSuite extends FunSuite {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]").set("spark.driver.memory", "8g")
    val sc = new SparkContext(conf)

    test("Shuffle") {
        val pairRdd = sc.parallelize(List((1, "1"), (2, "2")))

        val shuffledRDD = pairRdd.groupByKey()
        println(shuffledRDD.getClass)
    }

    val purchaseData = List(
        "1;DestX;15.4",
        "2;DestX;5.1",
        "2;DestY;10.4",
        "3;DestX;13.1",
        "3;DestY;23.2",
        "3;DestZ;5.7",
        "4;DestX;14.6",
        "4;DestY;5.5",
        "4;DestW;6.6",
        "4;DestQ;10",
        "5;DestX;2.1",
        "5;DestY;1.6",
        "5;DestW;14.5",
        "5;DestQ;5.9"
    )

    test("Shuffle CFFPurchase") {
        val purchasesRDD = sc.parallelize(purchaseData.map(CFFPurchase.parse))

        val purchasesPerMonth = purchasesRDD
                .map(p => (p.customerId, p.price))
                // this does shuffle over the network - slow
                .groupByKey()
                .mapValues(ps => (ps.size, ps.sum))
                .collect()

        purchasesPerMonth.foreach(println)

        assert(purchasesPerMonth.sorted === Array(
            (1, (1, 15.4)),
            (2, (2, 15.5)),
            (3, (3, 42.0)),
            (4, (4, 36.7)),
            (5, (4, 24.1))
        ))
    }

    test("Shuffle CFFPurchase optimized") {
        val purchasesRDD = sc.parallelize(purchaseData.map(CFFPurchase.parse))

        val purchasesPerMonth = purchasesRDD
                .map(p => (p.customerId, (1, p.price)))
                // faster (3x than orig on cluster), doesn't have to be sent across the network - reduced on the node
                .reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }
                // shuffling happens now but with much less data
                .collect()

        purchasesPerMonth.foreach(println)

        assert(purchasesPerMonth.sorted === Array(
            (1, (1, 15.4)),
            (2, (2, 15.5)),
            (3, (3, 42.0)),
            (4, (4, 36.7)),
            (5, (4, 24.1))
        ))
    }

    test("groupBy performance test") {
        val purchasesRDD = sc.parallelize(generateCFFPData).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val purchasesPerMonth = purchasesRDD
                .map(p => (p.customerId, p.price))
                // this does shuffle over the network - slow
                .groupByKey()
                .mapValues(ps => (ps.size, ps.sum))
                .collect()
    }

    test("reduceByKey performance test") {
        val purchasesRDD = sc.parallelize(generateCFFPData).persist(StorageLevel.MEMORY_AND_DISK_SER)

        val purchasesPerMonth = purchasesRDD
                .map(p => (p.customerId, (1, p.price)))
                // faster (3x than orig on cluster), doesn't have to be sent across the network - reduced on the node
                .reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }
                // shuffling happens now but with much less data
                .collect()
    }

    def generateCFFPData: List[CFFPurchase] = {
        val rand = Random

        val result = ListBuffer[CFFPurchase]()
        for (_ <- 1 to 5 * 1000 * 1000) {
            result += CFFPurchase(rand.nextInt(100), rand.nextString(10), rand.nextDouble() * 10)
        }

        result.toList
    }
}
