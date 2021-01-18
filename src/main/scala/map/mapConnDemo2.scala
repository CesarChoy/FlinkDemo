package map

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import redis.clients.jedis.Jedis

object mapConnDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = env.socketTextStream("localhost", 9999)
    input.map(new connMap)

    env.execute()
  }

  class connMap extends RichMapFunction[String, String] {
    var jedis: Jedis = _

    override def open(parameters: Configuration): Unit = {
      jedis = new Jedis("bigdata-test", 6379, 1000)
      println("jedis.isConnected" + jedis.isConnected)
      jedis.select(1)


    }

    override def map(value: String): String = {
      val strs = value.split(" ")
      jedis.set(strs(0), strs(1))
      println("jedis.isConnected" + jedis.isConnected)
      null
    }

    override def close(): Unit = {

      try {
        jedis.close()
      } catch {
        case e: Exception =>
      }

    }
  }


}
