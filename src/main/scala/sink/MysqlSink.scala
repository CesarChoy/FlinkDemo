package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MysqlSink() extends RichSinkFunction[stu] {
  var conn: Connection = _
  var inputStmt: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {

    parameters

    val parameterTool = getRuntimeContext.getExecutionConfig.getGlobalJobParameters.asInstanceOf[ParameterTool]
    val runType = parameterTool.get("runType")
    println("参数：" + runType)

    conn = DriverManager.getConnection("jdbc:mysql://bt-01:3306/test", "root", "123456")
    //    conn.setAutoCommit(false)
    inputStmt = conn.prepareStatement("insert into stu (id,name) values (?,?)")
  }

  override def invoke(value: stu, context: SinkFunction.Context[_]): Unit = {
    println("cccc=>" + stu)
    inputStmt.setInt(1, value.id)
    inputStmt.setString(2, value.name)
    inputStmt.execute()
    //    conn.commit()
  }

  override def close(): Unit = {
    inputStmt.close()
    conn.close()
    conn = null
  }


}