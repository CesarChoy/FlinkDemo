package tableapi

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import tableapi.tableDemo_AggregateFunction.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row


object tableDemo_TableFunction {
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
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'temp, 'timestamp.rowtime as 'ts)
    // 注册函数
    val split = new Split("_")
    tableEnv.registerFunction("split", split)
    tableEnv.createTemporaryView("sensorTable", sensorTable)


    //###################################################################################################################

    val resultTable = sensorTable
      .joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)


    /**
     * 一个字段炸裂成一张表
     */
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensorTable,lateral table(split(id)) as splitid(word,length)
        |""".stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("Sql")

    env.execute()
  }


  class Split(separator: String) extends TableFunction[(String, Int)] {
    def eval(str: String) = {
      // 炸裂
      str.split(separator).foreach(
        word => collect((word, word.length))
      )
    }
  }

}
