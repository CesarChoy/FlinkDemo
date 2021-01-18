package sink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

object KafkaSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._

    val input = env.socketTextStream("localhost", 9999)
    input.print()

    input.addSink(new FlinkKafkaProducer011[String]("10.0.12.95:9092", "testapi", new SimpleStringSchema()))
    env.execute("to kafka")
  }
}
