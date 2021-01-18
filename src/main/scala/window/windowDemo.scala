package window

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.java.tuple.{Tuple, Tuple4}
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

object windowDemo {
  val logger: Logger = LoggerFactory.getLogger("DataReportScala")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //修改并行度
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //checkpoint配置
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(30000)
    env.getCheckpointConfig.setCheckpointTimeout(10000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    //设置statebackend

    //env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop100:9000/flink/checkpoints",true))

    //隐式转换
    import org.apache.flink.api.scala._

    //获取kafka中的数据
    val data = env.socketTextStream("localhost", 9999)

    //对数据进行清洗
    val mapData = data.map(line => {
      val strs = line.split(" ")

      (strs(0).toLong, strs(1), strs(2))
    })

    //过滤掉异常数据
    val filterData = mapData.filter(_._1 > 0)

    //保存迟到太久的数据
    // 注意：针对java代码需要引入org.apache.flink.util.OutputTag
    //针对scala代码 需要引入org.apache.flink.streaming.api.scala.OutputTag
    val outputTag = new OutputTag[(Long, String, String)]("late-data") {}


    /**
     *
     * 1597974601000 0 1
     * 1597974631000 0 1
     * 1597974661000 0 1
     *
     * 总结 ：
     * 窗口开始 1597974600000
     * 窗口结束 1597974630000
     * 窗口开始时间自动设置为整点时间
     * 每条时间都会超时更新水位(最大允许的乱序时间是10s -watermark)
     * 水位超过窗口结束，则输出窗口计算
     *
     * 允许迟到30s -超时数据
     * 窗口超时 1597974660000 触发重新计算
     * 水位未超过窗口超时则 每条窗口内的数据都会重复重新计算
     */
    val resultData = filterData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, String)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s

      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)

      //携带事件时间
      override def extractTimestamp(element: (Long, String, String), previousElementTimestamp: Long): Long = {
        val timestamp = element._1
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        timestamp
      }
    }).keyBy(1, 2)
      .window(TumblingEventTimeWindows.of(Time.seconds(30)))
      .allowedLateness(Time.seconds(30)) //允许迟到30s
      .sideOutputLateData(outputTag) //收集迟到太久的数据
      .apply(new RichWindowFunction[(Long, String, String), Tuple4[String, String, String, Long], Tuple, TimeWindow] {

        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, String)], out: Collector[Tuple4[String, String, String, Long]]): Unit = {
          //获取分组字段信息，跟keyBy对应
          val type1 = key.getField(0).toString
          val area = key.getField(1).toString
          val it = input.iterator
          //存储时间，为了获取最后一条数据的时间
          val arrBuf = ArrayBuffer[Long]()
          var count = 0
          while (it.hasNext) {
            val next = it.next
            arrBuf.append(next._1)
            count += 1
          }
          println(Thread.currentThread.getId + ",window触发了，数据条数：" + count)
          println("窗口开始时间" + window.getStart + "窗口结束时间" + window.getEnd)

          //排序
          val arr = arrBuf.toArray
          Sorting.quickSort(arr)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val time = sdf.format(new Date(arr.last))

          //组装结果
          val res = new Tuple4[String, String, String, Long](time, type1, area, count)
          out.collect(res)
        }
      })

    resultData.print("ok!~")

    //获取迟到太久的数据
    resultData.getSideOutput[(Long, String, String)](outputTag).print("sideOutput")

    env.execute("win~~")

  }

}
