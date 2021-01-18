package source

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object setCheckpointDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 重启3次，每次间隔5秒
    // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    // 每隔10秒重启1次，2分钟超过3次则停止
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.minutes(2), Time.seconds(10)))

    //设置Checkpoint时间
    env.enableCheckpointing(10000L)
    val backend: StateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100)
    env.setStateBackend(backend)

    val socketStream = env.socketTextStream("localhost", 9999)

    import org.apache.flink.api.scala._
    val dataStream = socketStream
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    dataStream.print()

    //启动执行器，执行程序
    env.execute("启动~~")
  }
}
