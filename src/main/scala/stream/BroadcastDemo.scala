package stream

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

object BroadcastDemo {
  def main(args: Array[String]): Unit = {

    // 流执行环境
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 批执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    val conf = new Configuration()
    conf.setString("six", "6_")
    conf.setString("more", "out_")

    val broadcast_1 = env.fromElements("1_", "2_", "3_", "4_", "5_")

    val scStream = env.fromElements("as", "bbb", "z123c", "qwqqqq", "qweqweeerqweqwe")
    scStream.map(
      new RichMapFunction[String, (String, String)] {

        var bc: java.util.List[String] = _

        var six: String = _
        var more: String = _

        override def open(parameters: Configuration): Unit = {
          bc = getRuntimeContext.getBroadcastVariable("bc1")
          six = parameters.getString("six", "")
          more = parameters.getString("more", "")
        }

        override def map(str: String): (String, String) = {

          str.length match {
            case 1 => (str, bc.get(0).toString)
            case 2 => (str, bc.get(1).toString)
            case 3 => (str, bc.get(2).toString)
            case 4 => (str, bc.get(3).toString)
            case 5 => (str, bc.get(4).toString)
            case 6 => (str, six)
            case _ => (str, more)
          }
        }
      }
    ).withBroadcastSet(broadcast_1, "bc1")
      .withParameters(conf)

      .print()

    // 流执行
    //    env.execute("broadcast~~~")

  }
}
