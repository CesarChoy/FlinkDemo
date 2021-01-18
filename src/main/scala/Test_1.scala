import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, Tumble}

object Test_1 {
  def main(args: Array[String]): Unit =

    ExecutionEnvironment

  StreamTableEnvironment
  StreamExecutionEnvironment

  val input1: DataStream[(String, Int)] = null
  input1.keyBy()

  val input2: DataSet[(Int, String, Double)] = null
  val output2: DataSet[(Int, String, Double)] = input2
    .groupBy(1) // group DataSet on second field
    .minBy(0, 2) // select tuple with minimum values for first and third field.

  input2.join(input2)
    .where(0)
    .equalTo(0)


  /*    val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

      val streamFromFile: DataStream[String] = env.readTextFile("sensor.txt")
      val dataStream: DataStream[SensorReading] = streamFromFile
        .map(data => {
          val dataArray = data.split(",")
          SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
        })
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
        })

      val settings: EnvironmentSettings = EnvironmentSettings
        .newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build()
      val tableEnv: StreamTableEnvironment =
        StreamTableEnvironment.create(env, settings)

      val dataTable: Table = tableEnv
        .fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime)

      val resultTable: Table = dataTable
        .window(Tumble over 10.seconds on 'timestamp as 'tw)
        .groupBy('id, 'tw)
        .select('id, 'id.count)

      val sqlDataTable: Table = dataTable
        .select('id, 'temperature, 'timestamp as 'ts)
      val resultSqlTable: Table = tableEnv
        .sqlQuery("select id, count(id) from "
          + sqlDataTable
          + " group by id,tumble(ts,interval '10' second)")

      // 把 Table转化成数据流
      val resultDstream: DataStream[(Boolean, (String, Long))] = resultSqlTable
        .toRetractStream[(String, Long)]

      resultDstream.filter(_._1).print()
      env.execute()*/
}

case class SensorReading(aa: String, timestamp: Long, cc: Double){

}
