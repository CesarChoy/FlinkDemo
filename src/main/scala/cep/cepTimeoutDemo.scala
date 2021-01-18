package cep

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 1 create 1 1598088526000
 * 1 pay 2 1598088527000
 * 2 create 3 1598088528000
 * 2 pay 4 1598088529000
 * 3 create 5 1598088530000
 * 4 create 5 1598088532000
 * 4 pay 6 1598088535000
 * 5 create 7 1598088660000
 * 6 create 8 1598088800000
 * 6 pay 9 1598088860000
 *
 * 输出
 *
 * timeout> Warn(3,timeout,5)
 * timeout> Warn(5,timeout,7)
 * timeout> Warn(6,timeout,8)
 *
 */

object cepTimeoutDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._

    println("begin ~~~")

    val scStream = env.socketTextStream("localhost", 9999)
    val input = scStream
      .map(line => {
        val strs = line.split(" ")
        Stu(strs(0).toInt, strs(1), strs(2).toLong, strs(3).toLong)
      }).assignAscendingTimestamps(_.inTime)

    input.print()

    //匹配创建后 下单在10秒之内
    val pattern = Pattern.begin[Stu]("start")
      .where(_.status == "create")
      .next("next") //对于keyBy分组后，组内的数据
      .where(_.status == "pay")
      .within(Time.seconds(10))


    val badTag = OutputTag[Warn]("Bad")

    import scala.collection.Map

    val patternStream = CEP.pattern(input.keyBy(_.id), pattern)

    //没有匹配上则打标签
    val output = patternStream.select(badTag) {
      // 没有匹配上
      (pattern: Map[String, Iterable[Stu]], timestamp: Long) => {

        //取值，输出
        val s1 = pattern.get("start")
        Warn(s1.get.iterator.next().id, "timeout", s1.get.iterator.next().times)
      }
    } {

      // 匹配上
      (pattern: Map[String, Iterable[Stu]]) => {

        //取值，输出
        val s1 = pattern.get("start")
        val s2 = pattern.get("next")
        Warn(s1.get.iterator.next().id, "success", s1.get.iterator.next().times)
      }
    }

    output.getSideOutput(badTag).print("timeout")

    env.execute("~~~")

  }

  case class Stu(id: Int, status: String, times: Long, inTime: Long)

  case class Warn(id: Int, status: String, times: Long)

}
