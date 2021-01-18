import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import pojo.UserInfo;

import java.util.HashMap;
import java.util.Map;

public class test_2 {
    public static void main(String[] args) {
        UserInfo lucy = new UserInfo("005", "Lucy", 21);

        Gson gson = new Gson();
        String s = gson.toJson(lucy);

        Map<String, Object> map = JSONObject.parseObject(s, HashMap.class);

        System.out.println(map.get("userid"));
    }
}
