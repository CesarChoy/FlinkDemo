package window

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object windowJoinDemo {
  def main(args: Array[String]): Unit = {

    //获取参数,封装的HashMap
    val parTool = ParameterTool.fromArgs(args)
    val windowSize = parTool.getLong("windowSize", 2000)
    val raterate = parTool.getLong("rate", 5L)
    import org.apache.flink.api.scala._


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setGlobalJobParameters(parTool)

    //搁浅。原因 scala 的迭代器、Env 和 java不同，

    val scStream_lf = env.socketTextStream("localhost", 9999).map(words => {
      val strs = words.split(" ")
      (strs(0), strs(1).toInt)
    })
    val scStream_rt = env.socketTextStream("localhost", 8888).map(words => {
      val strs = words.split(" ")
      (strs(0), strs(1).toInt)
    })


    scStream_lf.join(scStream_rt)
      // scala 传递函数， java传递重写的对象
      .where(_._1)
      .equalTo(_._1)
      //时间窗口大小
      .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
      .apply((x, y) =>
        (x._1, x._2, y._2)
      ).print().setParallelism(1)

    env.execute("join join join ~~~~")
  }

}
