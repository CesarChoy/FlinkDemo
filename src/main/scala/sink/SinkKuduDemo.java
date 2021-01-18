package sink;

import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import pojo.UserInfo;

import java.util.HashMap;
import java.util.Map;

public class SinkKuduDemo {

    public static <IN> void main(String[] args) throws Exception {

        // 初始化 flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         * 离线
         */

        // 生成数据源
//        DataStreamSource dataSource = env.fromElements(new UserInfo("001", "Jack", 18),
//                new UserInfo("002", "Rose~~!!", 20),
//                new UserInfo("003", "Cris1230", 22),
//                new UserInfo("004", "Lily", 19),
//                new UserInfo("005", "Lucy", 21),
//                new UserInfo("006", "Json", 24));

        // 转换数据 map
//        SingleOutputStreamOperator<Map<String, Object>> mapSource = dataSource.map(new RichMapFunction<IN, HashMap>() {
//
//            Gson gson = null;
//
//            @Override
//            public void open(Configuration parameters) {
//                gson = new Gson();
//            }
//
//            @Override
//            public HashMap map(IN in) {
//                String json = gson.toJson(in);
//                return JSONObject.parseObject(json, HashMap.class);
//            }
//
//        });

        SingleOutputStreamOperator<Map<String, Object>> input = env.socketTextStream("localhost", 9999)
                .map(
                        new RichMapFunction<String, Map<String, Object>>() {
                            @Override
                            public Map<String, Object> map(String value) throws Exception {
                                String[] arr = value.split(",");
                                HashMap<String, Object> hm = new HashMap<>();
                                //userid: String, name: String, age: Int)
                                hm.put("userid", arr[0]);
                                hm.put("name", arr[1]);
                                hm.put("age", Integer.parseInt(arr[2]));
                                return hm;
                            }
                        });

        // sink 到 kudu
        String kuduMaster = "bt-01:7051,bt-02:7051,bt-03:7051";
        String tableInfo = "impala::kududb.userinfo";
//        mapSource.addSink(new SinkKudu(kuduMaster, tableInfo));
        input.addSink(new SinkKudu(kuduMaster, tableInfo));

        env.execute("sink-test");

    }
}
