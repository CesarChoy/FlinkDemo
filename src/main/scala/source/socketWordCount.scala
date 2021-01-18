package source

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object socketWordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //重启3次，每次间隔5秒
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))

    val socketStream2 = env.socketTextStream("localhost", 8888)
    socketStream2.print("zzz")

    val socketStream = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._
    val dataStream = socketStream
      //Java 写法
      //.flatMap(new LineSplitter())
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print()

    //启动执行器，执行程序
    env.execute("启动~~")
  }
}

class LineSplitter() extends RichFlatMapFunction[String, (String, Int)] {

  // 创建一个累加器
  private val intCounter = new IntCounter()

  override def open(parameters: Configuration): Unit = {
    getRuntimeContext.addAccumulator("ic", intCounter)
  }

  override def flatMap(str: String, out: Collector[(String, Int)]): Unit = {
    val strs = str.split(" ")
    for (s: String <- strs) {
      if (s.length != 0) {
        out.collect((s, 1))
      }
    }

    //累计处理行数
    intCounter.add(1)

  }
}
