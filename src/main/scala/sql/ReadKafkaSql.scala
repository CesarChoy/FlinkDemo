package sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object ReadKafkaSql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val tEnv = StreamTableEnvironment.create(env)

    val createTable =
      """
        |CREATE TABLE PERSON (
        |    id INT COMMENT 'id',
        |    name VARCHAR COMMENT '姓名'
        |) WITH (
        |    'connector.type' = 'kafka', -- 使用 kafka connector
        |    'connector.version' = '0.11',  -- kafka 版本
        |    'connector.topic' = 'testapi',  -- kafka topic
        |    'connector.startup-mode' = 'latest-offset', -- 从最新的 offset 开始读取
        |    'connector.properties.0.key' = 'bootstrap.servers',  -- 连接信息
        |    'connector.properties.0.value' = '10.0.12.95:9092',
        |    'connector.properties.1.key' = 'group.id',
        |    'connector.properties.1.value' = 'test',
        |    'update-mode' = 'append',
        |    'format.type' = 'csv',  -- 数据源格式为 json
        |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
        |)
        |""".stripMargin
    tEnv.sqlUpdate(createTable)

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

//    val query =
//      """
//        |SELECT * FROM PERSON
//        |""".stripMargin
//
//    val result = tEnv.sqlQuery(query)
//    //    result.toRetractStream[Row].print()
//    result.toAppendStream[Row].print()

    tEnv.sqlUpdate("INSERT INTO STU SELECT id,name FROM PERSON")

    tEnv.execute("Flink SQL DDL")
  }

}
