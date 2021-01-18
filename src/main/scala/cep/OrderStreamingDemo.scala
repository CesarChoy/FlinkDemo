package cep

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 订单实时预警
 */


object OrderStreamingDemo {

  case class OrderEvent(orderId: Long, eventType: String, eventTime: Long, id: Long)

  case class OrderResult(orderId: Long, eventType: String, id: Long)

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.api.scala._


    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842, 9001),
      //      OrderEvent(1, "create", 1558430813,9002), //模拟不在时间内
      OrderEvent(1, "create", 1558430813, 9002),
      OrderEvent(1, "pay", 1558430844, 9003),
      OrderEvent(2, "create", 1558430843, 9004),
      OrderEvent(2, "pay", 1558430844, 9005),
      OrderEvent(3, "create", 1558430844, 9006),
      OrderEvent(3, "pay", 1558430860, 9007)
    )).assignAscendingTimestamps(_.eventTime * 1000)

    // 定义一个带匹配时间窗口的模式
    val orderPayPattern = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .next("next")
      .where(_.eventType == "pay")
      .within(Time.seconds(15))

    // 定义一个输出标签
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")
    // 订单事件流根据 orderId 分流，然后在每一条流中匹配出定义好的模式
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

    //todo 需要导入scala Map
    import scala.collection.Map
    val complexResult = patternStream.select(orderTimeoutOutput) {
      //todo 统计超时的数据
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val createOrder = pattern.get("begin")
        OrderResult(createOrder.get.iterator.next().orderId, "timeout", createOrder.get.iterator.next().id)
      }
    } {
      // 检测到定义好的模式序列时，就会调用这个函数
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrder = pattern.get("next")
        val createOrder = pattern.get("begin")
        OrderResult(payOrder.get.iterator.next().orderId, "success", createOrder.get.iterator.next().id)
      }
    }
    // 拿到同一输出标签中的 timeout 匹配结果（流） 订单超时
    val timeoutResult = complexResult.getSideOutput(orderTimeoutOutput)

    //        complexResult.print()
    timeoutResult.print()

    env.execute("Order Timeout Detect Job")
  }


}
