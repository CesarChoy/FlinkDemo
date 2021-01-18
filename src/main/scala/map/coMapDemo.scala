package map

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object coMapDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    env.setParallelism(3)
    import org.apache.flink.api.scala._

    val cuStream_1 = env.addSource(getSourceFuntion(1000))
    val cuStream_2 = env.addSource(getSourceFuntion(2000))

    cuStream_1.print("cus->")

    // DataStream[Double]
    val new_cus_1 = cuStream_1.assignTimestampsAndWatermarks(getWatermark())
      .timeWindowAll(Time.of(5000, TimeUnit.MILLISECONDS))
      .apply(new AllWindowFunction[Int, Double, TimeWindow] {
        override def apply(window: TimeWindow, input: Iterable[Int], out: Collector[Double]): Unit = {
          out.collect(1.0)
        }
      })

    new_cus_1.print("new->")

    // DataStream[Int] map1() 处理输出 0
    // DataStream[Double] map2() 处理输出 1
    // 对两条流交互操作

    cuStream_2.connect(new_cus_1)
      .map(getCoMapFunction)
      .print("cuStream_2")

    //    cuStream_2:2> 0
    //    cus->:4> 1
    //    cuStream_2:3> 0
    //    cus->:5> 1

    env.execute("watermark & zz")

  }


  def getSourceFuntion(cnt: Int): SourceFunction[Int] = {
    new SourceFunction[Int] {
      var counter = 0

      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {

        while (counter < cnt) {
          ctx.collect(1)
          counter += 1
          Thread.sleep(500)
        }
      }

      override def cancel(): Unit = {
        // No cleanup needed
      }
    }
  }

  def getWatermark(): AssignerWithPunctuatedWatermarks[Int] = {
    new AssignerWithPunctuatedWatermarks[Int] {
      private val serialVersionUID = 1L

      var nextWatermark = 0L

      override def checkAndGetNextWatermark(lastElement: Int, extractedTimestamp: Long): Watermark = {
        new Watermark(nextWatermark - 1)
      }

      override def extractTimestamp(element: Int, previousElementTimestamp: Long): Long = {
        nextWatermark += 10
        nextWatermark
      }
    }
  }

  def getCoMapFunction: CoMapFunction[Int, Double, Int] = {
    new CoMapFunction[Int, Double, Int] {
      override def map1(value: Int): Int = {
        // Return newData

        0
      }

      override def map2(value: Double): Int = {
        // Update model
        1
      }
    }
  }

}
