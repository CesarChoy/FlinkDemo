package source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object customSourceDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._

    env.addSource(new SourceFunction[String] {
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        var i: Int = 0
        while (true) {
          i = i + 1
          ctx.collect("ccc" + i)

          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {

      }
    }).print()

    println("=====" + env.getExecutionPlan)
    env.execute()

  }
}
