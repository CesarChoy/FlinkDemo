package map

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

object MapStateDescriptorDemo2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 1分钟1次Checkpoint
    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1))
    val config = env.getCheckpointConfig
    // CheckPoint 语义 EXACTLY ONCE
    config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    import org.apache.flink.api.scala._

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "10.0.12.95:9092")
    prop.setProperty("group.id", "test")
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("auto.offset.reset", "latest")

    val kstream = env.addSource(new FlinkKafkaConsumer011[String]("testapi", new SimpleStringSchema(), prop))

    kstream.map(click => (click, 0L))
      .keyBy(0)
      .map(new RichMapFunction[(String, Long), (String, Long)] {

        //状态描述器接口
        var pvState: ValueState[Long] = _
        var pv: Long = 0

        override def open(parameters: Configuration): Unit = {

          //初始化状态描述器
          pvState = getRuntimeContext.getState(
            new ValueStateDescriptor[Long]("pvState", TypeInformation.of(new TypeHint[Long] {}))
          )
        }

        override def map(value: (String, Long)): (String, Long) = {
          if (null == pvState.value()) {
            pv = 1
          } else {
            pv = pvState.value()
            pv += 1
          }
          pvState.update(pv)
          (value._1, pv)
        }
      })
      .print()


    env.execute()
  }

}
