package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.ivi.opensource.flinkclickhousesink.ClickhouseSink;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseClusterSettings;
import ru.ivi.opensource.flinkclickhousesink.model.ClickhouseSinkConsts;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 *         <dependency>
 *             <groupId>ru.ivi.opensource</groupId>
 *             <artifactId>flink-clickhouse-sink</artifactId>
 *             <version>1.1.0</version>
 *         </dependency>
 *
 */

public class ClickhouseSinkDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        Map<String, String> globalParameters = new HashMap<>();
        // clickhouse cluster properties
        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_HOSTS, "http://bt-05:8123/tmp");
//        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_USER, "root");
//        globalParameters.put(ClickhouseClusterSettings.CLICKHOUSE_PASSWORD, "123456");

        // sink common
        globalParameters.put(ClickhouseSinkConsts.TIMEOUT_SEC, "10");
        globalParameters.put(ClickhouseSinkConsts.FAILED_RECORDS_PATH, "D:\\project_demo\\flink_demo\\src\\main\\resources");
        globalParameters.put(ClickhouseSinkConsts.NUM_WRITERS, "1");
        globalParameters.put(ClickhouseSinkConsts.NUM_RETRIES, "3");
        globalParameters.put(ClickhouseSinkConsts.QUEUE_MAX_CAPACITY, "3");

        // set global paramaters
        ParameterTool parameters = ParameterTool.fromMap(globalParameters);
        env.getConfig().setGlobalJobParameters(parameters);

        DataStreamSource<String> input = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> out = input.map(new MapFunction<String, String>() {
            @Override
            public String map(String in) throws Exception {
                return convertToCsv(in);
            }
        });

        // create props for sink
        Properties props = new Properties();
        props.put(ClickhouseSinkConsts.TARGET_TABLE_NAME, "tmp.id_val");
        props.put(ClickhouseSinkConsts.MAX_BUFFER_SIZE, "2");
        out.addSink(new ClickhouseSink(props));

        env.execute();
    }

    public static String convertToCsv(String in) {
        String[] arr = in.split(",");

        StringBuilder builder = new StringBuilder();
        builder.append("(");

        // add a.str
        // builder.append("'");
        // builder.append(a.str);
        // builder.append("', ");

        // add a.intger
        builder.append(String.valueOf(arr[0]));
        builder.append(", ");
        builder.append(String.valueOf(arr[1]));
        builder.append(" )");
        return builder.toString();
    }


}