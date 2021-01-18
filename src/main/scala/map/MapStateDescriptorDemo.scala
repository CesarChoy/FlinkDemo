package map

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

object MapStateDescriptorDemo {

  //初始化状态描述器，抽出来的原因时因为多个流？？？
  private val rule = new MapStateDescriptor[String, String]("rule", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._

    //存入状态描述器
//    val bcStream = env.fromElements("a", "b", "c").broadcast(rule)
    val bcStream = env.socketTextStream("localhost", 8888).broadcast(rule)

    val scStream = env.socketTextStream("localhost", 9999)
      .connect(bcStream) //连接状态流/维度表流
      .process(new BroadcastProcessFunction[String, String, String] {
        override def processElement(str: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {

          //获取状态描述器
          val realOnlyRule = ctx.getBroadcastState(rule)
          if (realOnlyRule.contains(str)) {
            out.collect(str + " has contains!")
          }
        }

        override def processBroadcastElement(str: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {

          //获取状态描述器（可读写,即更新）
          val writeRule = ctx.getBroadcastState(rule)
          writeRule.put(str, str)
        }
      })

    scStream.print()

    env.execute()
  }
}
