package window

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object windowOnTimer {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改并行度
    env.setParallelism(1)
    //process时间不能用，会有空指针异常
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    //隐式转换
    import org.apache.flink.api.scala._

    //获取kafka中的数据
    val data = env.socketTextStream("localhost", 9999)
    //对数据进行清洗
    val stream: DataStream[(String, String)] = data.map(line => {
      val strs = line.split(" ")
      (strs(0), strs(1))
    })

    // apply the process function onto a keyed stream
    val result: DataStream[(String, Long)] = stream
      .keyBy(0)
      .process(new CountWithTimeoutFunction())

    result.print()

    env.execute(" abababa~~")
  }


  /**
   * The data type stored in the state
   */
  case class CountWithTimestamp(key: String, count: Long, lastModified: Long)

  /**
   * The implementation of the ProcessFunction that maintains the count and timeouts
   */
  class CountWithTimeoutFunction extends KeyedProcessFunction[Tuple, (String, String), (String, Long)] {

    /** The state that is maintained by this process function */
    lazy val state: ValueState[CountWithTimestamp] = getRuntimeContext
      .getState(new ValueStateDescriptor[CountWithTimestamp]("myState", classOf[CountWithTimestamp]))

    override def processElement(
                                 value: (String, String),
                                 ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#Context,
                                 out: Collector[(String, Long)]): Unit = {

      // initialize or retrieve/update the state
      val current: CountWithTimestamp = state.value match {
        case null =>
          CountWithTimestamp(value._1, 1, ctx.timestamp())
        case CountWithTimestamp(key, count, lastModified) =>
          CountWithTimestamp(key, count + 1, ctx.timestamp)
      }

      // write the state back
      state.update(current)

      // schedule the next timer 60 seconds from the current event time
      ctx.timerService.registerEventTimeTimer(current.lastModified + 20000)
    }

    override def onTimer(
                          timestamp: Long,
                          ctx: KeyedProcessFunction[Tuple, (String, String), (String, Long)]#OnTimerContext,
                          out: Collector[(String, Long)]): Unit = {

      state.value match {
        case CountWithTimestamp(key, count, lastModified) if (timestamp == lastModified + 20000) =>
          out.collect((key, count))
        case _ =>
      }
    }
  }

}
