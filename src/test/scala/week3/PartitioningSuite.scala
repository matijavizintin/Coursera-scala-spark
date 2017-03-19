package week3

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by matijav on 14/03/2017.
  */
@RunWith(classOf[JUnitRunner])
class PartitioningSuite extends FunSuite {
    val conf: SparkConf = new SparkConf().setAppName("example").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // smart partitioning is even faster than using reduceByKey (9x from orig on cluster)
    test("Partitioning") {
        val pairs = sc.parallelize(generateCFFPData(100000)).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        partitioned.collect()
    }

    test("Partitioning operations") {
        val pairs = sc.parallelize(generateCFFPData(100000)).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        // doesn't preserve a partitioner since we can change keys and the order wouldn't make any sense
        partitioned.map {
            case (k, v) => "dummy"
        }

        // preserves the partitioner since we don't change the keys
        partitioned.mapValues(v => v + 1)
    }

    test("Partitioning operations 2") {
        val pairs = sc.parallelize(generateCFFPData(100000)).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        partitioned.map {
            case (cId, p) => (cId, (1, p))
        }.reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.collect()

        // better - preserves the partitioner
        partitioned.mapValues(p => (1, p)).reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.collect()
    }

    test("Debug") {
        val pairs = sc.parallelize(generateCFFPData(100)).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        val debug = partitioned.map {
            case (cId, p) => (cId, (1, p))
        }.reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.toDebugString

        println(debug)
        println()

        // better
        val debug2 = partitioned.mapValues(p => (1, p)).reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.toDebugString

        println(debug2)
    }

    test("Dependencies") {
        val pairs = sc.parallelize(generateCFFPData(100)).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        println("After partionBy")
        partitioned.dependencies.foreach(println)

        val mapped = partitioned.map {
            case (cId, p) => (cId, (1, p))
        }
        println("After map")
        mapped.dependencies.foreach(println)

        val reduced = mapped.reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.persist()
        println("After reduceByKey")
        reduced.dependencies.foreach(println)

        val grouped = reduced.groupByKey()
        println("After groupByKey")
        grouped.dependencies.foreach(println)
    }

    def generateCFFPData(size: Int): List[CFFPurchase] = {
        val rand = Random

        val result = ListBuffer[CFFPurchase]()
        for (_ <- 1 to size) {
            result += CFFPurchase(rand.nextInt(100), rand.nextString(10), rand.nextDouble() * 10)
        }

        result.toList
    }
}
