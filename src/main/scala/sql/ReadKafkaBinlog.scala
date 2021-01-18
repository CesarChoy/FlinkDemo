package sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object ReadKafkaBinlog {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val tEnv = StreamTableEnvironment.create(env)

    val createTable =
      """
        |CREATE TABLE PERSON (
        |    database STRING COMMENT '库名',
        |    `table` STRING COMMENT '表名',
        |    type STRING COMMENT '操作类型',
        |    ts BIGINT COMMENT '时间戳',
        |    xid BIGINT COMMENT '事务',
        |    `commit` BOOLEAN COMMENT '提交',
        |    data ROW <Id INT,
        |              trade_no STRING,
        |              orderno STRING,
        |              OrderType INT,
        |              FuserId INT,
        |              Fusername STRING,
        |              TuserId INT,
        |              Tusername STRING,
        |              payment_free DOUBLE,
        |              payment_status INT,
        |              payment_time STRING,
        |              MinisnsId INT,
        |              Articleid INT,
        |              Status INT,
        |              ActionType INT,
        |              `Percent` INT,
        |              CommentId INT,
        |              OperStatus INT,
        |              remark STRING,
        |              confirm_time STRING,
        |              Addtime STRING,
        |              ShowNote STRING,
        |              AttachPar STRING,
        |              userip STRING,
        |              appid STRING,
        |              mch_id STRING,
        |              RelayId INT,
        |              applyId INT,
        |              VirtualFee INT,
        |              refundmoney BIGINT,
        |              ModifyDate  STRING>
        |) WITH (
        |    'connector.type' = 'kafka', -- 使用 kafka connector
        |    'connector.version' = '0.11',  -- kafka 版本
        |    'connector.topic' = 'maxwell_morders',  -- kafka topic
        |    'connector.startup-mode' = 'latest-offset', -- 从最新的 offset 开始读取
        |    'connector.properties.0.key' = 'bootstrap.servers',  -- 连接信息
        |    'connector.properties.0.value' = '10.0.12.95:9092',
        |    'connector.properties.1.key' = 'group.id',
        |    'connector.properties.1.value' = 'test',
        |    'update-mode' = 'append',
        |    'format.type' = 'json',  -- 数据源格式为 json
        |    'format.derive-schema' = 'true' -- 从 DDL schema 确定 json 解析规则
        |)
        |""".stripMargin
    tEnv.sqlUpdate(createTable)

    val query =
      """
        |SELECT * FROM PERSON
        |""".stripMargin
    val result = tEnv.sqlQuery(query)

    //    result.toRetractStream[Row].print()
    result.toAppendStream[Row].print()


    tEnv.execute("Flink SQL DDL")
  }

}
