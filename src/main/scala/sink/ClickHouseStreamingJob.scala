package sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.sources._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api._
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink

/**
 * 离线
 *
 * <dependency>
 * <groupId>ru.yandex.clickhouse</groupId>
 * <artifactId>clickhouse-jdbc</artifactId>
 * <version>0.2.4</version>
 * </dependency>
 *
 */

object StreamingJob {
  def main(args: Array[String]) {
    val SourceCsvPath =
      "D:\\project_demo\\flink_demo\\src\\main\\resources\\id_vid.csv"
    val CkJdbcUrl =
      "jdbc:clickhouse://bt-05:8123/tmp"
    //    val CkUsername = "root"
    //    val CkPassword = "123456"
    val BatchSize = 500 // 设置您的batch size

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = StreamTableEnvironment.create(env)

    val csvTableSource = CsvTableSource
      .builder()
      .path(SourceCsvPath)
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .field("id", DataTypes.INT)
      .field("val", DataTypes.INT)
      .build()

    tEnv.registerTableSource("source", csvTableSource)

    val resultTable = tEnv.scan("source").select("id, val")

    val insertIntoCkSql =
      """
        |  INSERT INTO id_val (
        |    id, val
        |  ) VALUES (
        |    ?, ?
        |  )
      """.stripMargin

    //将数据写入 ClickHouse Sink
    val sink = JDBCAppendTableSink
      .builder()
      .setDrivername("ru.yandex.clickhouse.ClickHouseDriver")
      .setDBUrl(CkJdbcUrl)
      //      .setUsername(CkUsername)
      //      .setPassword(CkPassword)
      .setQuery(insertIntoCkSql)
      .setBatchSize(BatchSize)
      .setParameterTypes(Types.INT, Types.INT())
      .build()

    tEnv.registerTableSink(
      "sink",
      Array("id", "val"),
      Array(Types.INT, Types.INT),
      sink
    )

    tEnv.insertInto(resultTable, "sink")

    env.execute("Flink Table API to ClickHouse Example")
  }
}