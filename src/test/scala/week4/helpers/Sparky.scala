package week4.helpers

import org.scalatest.FunSuite

/**
  * Created by matijav on 22/03/2017.
  */
abstract class Sparky extends FunSuite {
    // disable spark logs
    import org.apache.log4j.{Level, Logger}
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
}
