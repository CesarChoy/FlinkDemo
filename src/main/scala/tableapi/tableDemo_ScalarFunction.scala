package tableapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import tableapi.tableDemo_AggregateFunction.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

object tableDemo_ScalarFunction {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = StreamTableEnvironment.create(env)
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
     * UTF 字段转换
     */
    //流转表，定义时间字段
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp.rowtime as 'ts)
    //注册表
    tableEnv.createTemporaryView("sensor", sensorTable)
    //注册函数
    val hashCode = new HashCode(1.2)
    tableEnv.registerFunction("hashCode", hashCode)


    //###################################################################################################################


    //Table Api
    val resultTable = sensorTable.select('id, 'ts, hashCode('id))

    //Table Sql Api
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,hashCode(id)
        |from sensor
        |""".stripMargin)

    resultTable.toAppendStream[Row].print("sql")
    resultSqlTable.toAppendStream[Row].print("result")


    env.execute()

  }

  class HashCode(factor: Double) extends ScalarFunction {
    def eval(value: String): Int = {
      (value.hashCode * factor).toInt
    }
  }

}