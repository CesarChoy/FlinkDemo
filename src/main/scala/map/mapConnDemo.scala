package map

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object mapConnDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val input = env.socketTextStream("localhost", 9999)
    input.map(new connMap)

    env.execute(" map save to mysql")
  }


  class connMap extends RichMapFunction[String, String] {
    var conn: Connection = _
    var inputStmt: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://bt-01:3306/test", "root", "123456")
      inputStmt = conn.prepareStatement("insert into stu (id,name) values (?,?)")
    }


    override def map(value: String): String = {
      val strs = value.split(" ")
      println("cccc=>" + strs)
      inputStmt.setInt(1, strs(0).toInt)
      inputStmt.setString(2, strs(1))
      inputStmt.execute()

      null
    }
  }

}
