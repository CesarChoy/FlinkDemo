package tableapi

import org.apache.flink.api.scala.ExecutionEnvironment

object dataSetDemo {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()
    env.setParallelism(1)

    val data = env.readTextFile("file:///D:\\project_demo\\flink_demo\\src\\main\\resources\\Country_Drink.csv")

    data.filter(line => {
      val strs = line.split(",")
      "China".equals(strs(0))
    }).writeAsText("file:///D:\\project_demo\\flink_demo\\src\\main\\resources\\out")

    env.execute("bad ??")

  }
}
