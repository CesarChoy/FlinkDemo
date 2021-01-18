package sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 离线
 */
object ReadMysqlSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tEnv = StreamTableEnvironment.create(env)

    val createSinkTable =
      """
        |CREATE TABLE STU (
        |    id INT,
        |    name VARCHAR
        |) WITH (
        |'connector.type' = 'jdbc', -- 使用 jdbc connector
        |'connector.url' = 'jdbc:mysql://bt-01:3306/test', -- jdbc url
        |'connector.table' = 'stu', -- 表名
        |'connector.username' = 'root', -- 用户名
        |'connector.password' = '123456', -- 密码
        |'connector.write.flush.max-rows' = '1' -- 默认5000条，为了演示改为1条
        |)
        |""".stripMargin
    tEnv.sqlUpdate(createSinkTable)

    val query =
      """
        |SELECT * FROM STU
        |""".stripMargin

    val result = tEnv.sqlQuery(query)
    //    result.toRetractStream[Row].print()
    result.toAppendStream[Row].print()

    tEnv.execute("Flink SQL DDL")
  }

}
