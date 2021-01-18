import org.apache.flink.api.java.functions.KeySelector

import scala.util.Random

object test_1 {
  def main(args: Array[String]): Unit = {

    println(System.currentTimeMillis())
    val random = new Random()
    println(System.currentTimeMillis() + random.nextInt(10000))



  }
}
