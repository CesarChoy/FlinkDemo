package source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object kafkaWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "10.0.12.95:9092")
    prop.setProperty("group.id", "test")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    val kstream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), prop))
    val kwstream = kstream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      //      .sum(1)
      //      .reduce(new ReduceFunction[(String, Int)] {
      //        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      //          (value1._1, value1._2 + value2._2)
      //        }
      //      })
      .reduce((x, y) =>
        (x._1, x._2 + y._2)
      )

    kwstream.print()

    env.execute()

  }

}
