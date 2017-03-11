package week1

/**
  * Created by matijav on 11/03/2017.
  */
abstract class RDD[T] {

    def map[U](f: T => U): RDD[U] = ???

    def flatMap[U](f: T => TraversableOnce[U]): RDD[U] = ???

    def filter(pred: T => Boolean): RDD[T] = ???

    def reduce(op: (T, T) => T): RDD[T] = ???

}
