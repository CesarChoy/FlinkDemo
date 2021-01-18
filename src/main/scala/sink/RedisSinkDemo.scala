package sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig

object RedisSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = env.socketTextStream("localhost", 9999)
      .map(line => {
        val strs = line.split(" ")
        cls(strs(0).toInt, strs(1))
      })


    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("bigdata-test")
      .setPort(6379)
      .setDatabase(1)
      .build()

    input.addSink(new RedisSink[cls](conf, new redisMapper))

    env.execute("redis hset~~~")
  }
}

case class cls(id: Int, name: String)

class redisMapper extends RedisMapper[cls] {
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "cls_tmp")
  }

  override def getKeyFromData(t: cls): String = t.id.toString

  override def getValueFromData(t: cls): String = t.name
}