package map

import java.util
import java.util.Date

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * 目的：实现 窗口内计数，超出则打印数据
 */
object ProcessFunctionDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 设置事件为EventTime (默认为processTime)，配合watermark使用
    // 一个元素生成一个watermark，当watermark大于某个元素的触发时间，onTimer执行
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 指定周期性水印
    //    env.getConfig.setAutoWatermarkInterval(1000)
    import org.apache.flink.api.scala._
    // 指定乱序窗口长度
    val maxOutOfOrderness = 1000L

    val data = env.addSource(getEventTimeSource).setParallelism(1)

    //侧输出标签OutputTag
    val later_tag = new OutputTag[(String, Long)]("later_tag")

    val stream = data

      //打上时间水印后丢掉
      .assignTimestampsAndWatermarks(
        //允许有界乱序，传入的参数maxOutOfOrderness就是乱序区间的长度
        new BoundedOutOfOrdernessTimestampExtractor[(String, Long, Long)](Time.milliseconds(maxOutOfOrderness)) {
          //水印
          override def extractTimestamp(element: (String, Long, Long)): Long = {
            element._3
          }
        })
      .map(x => (x._1, x._2))
      .keyBy(x => x._1)
      //                  .process()

      //没处理完~~~？？？？？？？
      .process(getProcessFuntion)
      .print()
    //窗口+延迟 不过吃资源
    //      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
    //      .allowedLateness(Time.seconds(10))
    //      .sideOutputLateData(later_tag)


    env.execute("process ")
  }

  def getEventTimeSource: RichParallelSourceFunction[(String, Long, Long)] = {
    new RichParallelSourceFunction[(String, Long, Long)] {
      var running = true

      override def run(ctx: SourceFunction.SourceContext[(String, Long, Long)]): Unit = {

        val random = new Random()
        while (running) {
          var word = random.nextInt(20).toString
          var eventTime = System.currentTimeMillis() + random.nextInt(10000)
          ctx.collect((word, 1L, eventTime))

          //打印数据源
          println((word, eventTime / 1000))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    }
  }

  def getProcessFuntion: ProcessFunction[(String, Long), (String, Long)] = {
    new ProcessFunction[(String, Long), (String, Long)] {

      //      var state: ValueState[util.HashMap[String, String]] = _
      var mapstate: MapState[String, String] = _

      //初始化状态值
      override def open(parameters: Configuration): Unit = {
        //        state = getRuntimeContext.getState(
        //          new ValueStateDescriptor[util.HashMap[String, String]](
        //            "mapstate", TypeInformation.of(new TypeHint[util.HashMap[String, String]] {})))

        mapstate = getRuntimeContext.getMapState(new MapStateDescriptor[String, String](
          "", classOf[String], classOf[String])
        )
      }

      override def processElement(in: (String, Long), ctx: ProcessFunction[(String, Long), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
        if (mapstate.isEmpty) {
          mapstate.put("key", in._1)
          mapstate.put("count", "0")
        }

        val cnt = mapstate.get("count")
        val new_cnt = cnt + 1
        mapstate.put("count", new_cnt.toString)
        //存放处理时间
        mapstate.put("time", ctx.timestamp().toString)

        println(mapstate.get("key") + ":" + mapstate.get("count") + ":" + mapstate.get("time").toLong + ":" + mapstate.isEmpty)

        //注册定时器,更新一次就会有一个ProcessTimer
        ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 10000L) //10秒

      }

      //触发定时器
      override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, Long), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
        println("in_onTimer:" + timestamp)

        //        val hm = state.value()
        println(mapstate.get("key") + ":" + mapstate.get("count") + ":" + mapstate.get("time").toLong + ":" + mapstate.isEmpty)

        val time = mapstate.get("time").toLong

        // 当前最大的 watermark >= 最后一次更新时间 超过 60s则 执行
        if (timestamp >= time + 5000) { //5秒
          println("定时器被触发：" + new Date(timestamp / 1000) + mapstate.get("key") + "~~" + mapstate.get("count") + "~~" + mapstate.get("time"))
          out.collect((mapstate.get("key"), mapstate.get("count").toLong))

        } else {
          println("定时器没有被触发：" + new Date(timestamp) + mapstate.get("key") + "~~" + mapstate.get("count") + "~~" + mapstate.get("time"))
        }

      }


    }


  }

}
