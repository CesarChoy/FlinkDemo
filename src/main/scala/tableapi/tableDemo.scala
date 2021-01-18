package tableapi

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

object tableDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //    val Settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    val inputStream = env.readTextFile("file:///D:\\project_demo\\flink_demo\\src\\main\\resources\\Country_Drink.csv")
      .map(line => {
        val strs = line.split(",")
        countryDrink(strs(0), strs(1), strs(2))
      })

    // ######################################################################################################################


    val table_input = tableEnv.fromDataStream(inputStream)

    //打印TABLE API 添加到流
    val table_China = table_input.filter("country ==='China'")
    tableEnv.toAppendStream[countryDrink](table_China).print("out")

    //打印TABLE SQL
    tableEnv.createTemporaryView("table_input", table_input)
    val table_China_2 = tableEnv.sqlQuery(
      """
        |select * from
        |table_input
        |where country = 'China'
        |""".stripMargin)
    tableEnv.toAppendStream[countryDrink](table_China_2).print("out_2")


    // ######################################################################################################################


    //定义结构
    val schema = new Schema()
      .field("country", DataTypes.STRING())
      .field("drink", DataTypes.STRING())
      .field("timestamp", DataTypes.STRING())

    //设置输出格式
    tableEnv.connect(new FileSystem().path("file:///D:\\project_demo\\flink_demo\\src\\main\\resources\\out_table"))
      .withFormat(new Csv().fieldDelimiter('|'))
      .withSchema(schema)
      .createTemporaryTable("table_China_2")
    //输出到文件
    table_China_2.insertInto("table_China_2")

    env.execute(" begin ~~ ")

  }

  case class countryDrink(val country: String, val drink: String, val date: String)

}
