import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

public class Test01 {
    public static void main(String[] args) {
        JSONObject jsonObject = JSON.parseObject("{\"#account_id\":[\"_accountid\", \"String\"],\"#distinct_id\":[\"_distinctid\", \"String\"],\"#event_name\":[\"_eventname\", \"String\"],\"#ip\":[\"_ip\", \"String\"],\"#time\":[\"_time\", \"Date\"],\"#zone_offset\":[\"\", \"Zone\"],\"app_plat\":[\"app_plat\", \"String\"],\"ad_channel\":[\"ad_channel\", \"String\"],\"uin\":[\"uin\", \"String\"],\"group_id\":[\"group_id\", \"String\"],\"level\":[\"level\", \"Number\"],\"total_pay_money\":[\"total_pay_money\", \"Number\"],\"device_os\":[\"device_os\", \"String\"],\"honour\":[\"honour\", \"Number\"],\"backflow_channel\":[\"backflow_channel\", \"String\"],\"share_src_ad_channel\":[\"share_src_ad_channel\", \"String\"],\"config_group\":[\"config_group\", \"String\"],\"app\":[\"_app\", \"String\"],\"rating_2v2\":[\"rating_2v2\", \"Number\"],\"honour\":[\"honour\", \"Number\"],\"score\":[\"score\", \"Number\"],\"duration\":[\"number\", \"Number\"],\"teammate\":[\"match_teammate\", \"String\"],\"mode\":[\"mode\", \"String\"],\"hero_num\":[\"hero_num\", \"Number\"]}");

        JSONObject jsonObject1 = JSON.parseObject("{\"_accountid\":\"1_1024\",\"_app\":\"yxs_test\",\"_distinctid\":\"1_1024\",\"_eventname\":\"match_info\",\"_ip\":\"192.168.2.17\",\"_keyid\":\"KuqeSSvw\",\"_time\":1658249302771,\"ad_channel\":\"\",\"app_plat\":1,\"backflow_channel\":\"\",\"backflow_ten_day_channel\":\"\",\"config_group\":1,\"device_os\":\"\",\"group_id\":1,\"hero_num\":116,\"honour\":1036,\"level\":188,\"match_teammate\":0,\"mode\":4,\"number\":11,\"rating_2v2\":1000,\"score\":-4,\"share_src_ad_channel\":\"\",\"time\":\"1658220502\",\"total_pay_money\":0,\"uin\":1024}");

        String beforeKey = "{\n" +
                "            \"change_type\":[\"\", \"CHANGETYPE\"],\n" +
                "            \"item_type\":[\"item_type\", \"String\"],\n" +
                "            \"item_id\":[\"item_id\", \"String\"],\n" +
                "            \"old_expire\":[\"old_expire\", \"Number\"],\n" +
                "            \"new_expire\":[\"new_expire\", \"Number\"],\n" +
                "            \"before_num\":[\"old_cnt\", \"Number\"],\n" +
                "            \"after_num\":[\"new_cnt\", \"Number\"],\n" +
                "            \"change_num\":[\"change_cnt\", \"Number\"],\n" +
                "        }";

        JSONObject after_before_json = JSON.parseObject(beforeKey);

        //建立一个新的JSONObject，存储修改后信息
        JSONObject resultSubJson = new JSONObject();
        //获取映射后子字段名
        Set<String> afterSubkeys = after_before_json.keySet();
        //遍历映射后的子字段名，按照数数规则处理格式




        //获取映射后子字段类型



    }

    public static boolean isJSON(String str) {
        boolean result = false;
        try {
            JSONObject jsonObject = JSON.parseObject(str);
            result = true;
        } catch (Exception e) {
            result=false;
        }
        return result;
    }
}
