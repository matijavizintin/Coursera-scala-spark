package week3

/**
  * Created by matijav on 14/03/2017.
  */
case class CFFPurchase(customerId: Int, destination: String, price: Double) {

}

object CFFPurchase {
    def parse(line: String): CFFPurchase = {
        val array = line.split(";")
        CFFPurchase(array(0).toInt, array(1), array(2).toDouble)
    }
}
