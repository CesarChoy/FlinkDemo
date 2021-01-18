package stream

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object outputTag {

  private val small = new OutputTag[String]("small")
  private val mid = new OutputTag[String]("mid")

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment


    val stream = env.socketTextStream("localhost", 9999)
      .process(getProcess())

    //拆分流
    stream.getSideOutput(small).print("small")
    stream.getSideOutput(mid).print("mid")
    stream.getSideOutput(new OutputTag[String]("big")).print("big")

    env.execute("outputTag~~")

  }

  def getProcess() = {
    new ProcessFunction[String, (String, Int)] {


      override def processElement(in: String, ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        val strs = in.split(",")
        for (str: String <- strs) {
          if (str.length <= 3) {
            ctx.output(small, str)
          } else if (str.length <= 6) {
            ctx.output(mid, str)
          } else {
            ctx.output(new OutputTag[String]("big"), str)
          }
          out.collect((str, 1))
        }
      }
    }
  }
}
