package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.api.java.utils.ParameterTool

object MysqlSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val parameterTool = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(parameterTool)

    val input = env.socketTextStream("localhost", 9999)
      .map(line => {
        val strs = line.split(",")
        stu(strs(0).toInt, strs(1))
      })
    input.print()

    input.addSink(new MysqlSink())

    env.execute("insert into mysql")
  }
}

case class stu(id: Int, name: String)