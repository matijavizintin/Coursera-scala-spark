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
        val pairs = sc.parallelize(generateCFFPData).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        partitioned.collect()
    }

    test("Partitioning operations") {
        val pairs = sc.parallelize(generateCFFPData).map(p => (p.customerId, p.price))

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
        val pairs = sc.parallelize(generateCFFPData).map(p => (p.customerId, p.price))

        val tunedPartitioner = new RangePartitioner(8, pairs)
        val partitioned = pairs
                .partitionBy(tunedPartitioner)
                .persist() // partition only once - otherwise is partitioned each time

        partitioned.map {
            case (cId, p) => (cId, (1, p))
        }.reduceByKey {
            case ((cnt1, p1), (cnt2, p2)) => (cnt1 + cnt2, p1 + p2)
        }.collect()
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
