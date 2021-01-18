package tableapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object tableDemo_AggregateFunction {

  case class SensorReading(id: String, timestamp: Long, temp: Double)

  case class AggTempAcc(var sum: Double = 0.0, var count: Int = 0)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val input = env.readTextFile("D:\\project_demo\\flink_demo\\src\\main\\resources\\sensor.txt")
    val dataStream = input.map(line => {
      val strs = line.split(",")
      SensorReading(strs(0), strs(1).toLong, strs(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })


    //###################################################################################################################

    /**
     * 维护状态，一个字段炸裂成一张表
     */
    //流转表，定义时间字段
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp.rowtime as 'ts)
    //注册函数
    val avgtemp = new AvgTemp


    //###################################################################################################################


    val resultTable: Table = sensorTable
      .groupBy('id)
      .aggregate(avgtemp('temp) as 'avgTemp)
      .select('id, 'avgTemp)

    //表转流
    resultTable.toRetractStream[Row].print("result")
    //result> (true,aa,30.0)
    //result> (false,aa,30.0)

    env.execute("stream -> table -> stream")

  }

  class AvgTemp extends AggregateFunction[Double, AggTempAcc] {
    override def getValue(accumulator: AggTempAcc): Double = {
      accumulator.sum / accumulator.count
    }

    override def createAccumulator(): AggTempAcc = {
      new AggTempAcc()
    }

    //累加 at least one method named 'accumulate'
    def accumulate(acc: AggTempAcc, temp: Double): Unit = {
      acc.sum += temp
      acc.count += 1
    }
  }


}
