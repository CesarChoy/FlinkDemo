package tableapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import tableapi.tableDemo_AggregateFunction.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object tableDemo_TableAggregateFunction {

  case class TopTempAcc(var highestTemp: Double = Long.MinValue, var secondHighestTemp: Double = Long.MinValue)

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


    // 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp.rowtime as 'ts)
    // 注册函数
    val top2Temp = new Top2Temp()


    //###################################################################################################################


    /**
     * 维护状态，一个字段炸裂成一张表
     */
    val resultTable = sensorTable.groupBy('id)
      .flatAggregate(top2Temp('temp) as('temp, 'rank))
      .select('id, 'temp, 'rank)

    resultTable.toRetractStream[Row].print("result")

    env.execute()

  }

  class Top2Temp() extends TableAggregateFunction[(Double, Int), TopTempAcc] {
    //初始化
    override def createAccumulator(): TopTempAcc = new TopTempAcc()

    def accumulate(acc: TopTempAcc, temp: Double) = {
      if (temp > acc.highestTemp) {
        acc.secondHighestTemp = acc.highestTemp
        acc.highestTemp = temp
      } else if (temp > acc.secondHighestTemp) {
        acc.secondHighestTemp = temp
      }
    }

    def emitValue(acc: TopTempAcc, out: Collector[(Double, Int)]): Unit = {
      // 炸裂
      out.collect(acc.highestTemp, 1)
      out.collect(acc.secondHighestTemp, 2)
    }

  }

}
