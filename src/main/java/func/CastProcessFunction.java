package func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Set;

public class CastProcessFunction extends BroadcastProcessFunction<String, String, String> {

    private MapStateDescriptor<String, String> mapStateDescriptor;

    public CastProcessFunction(MapStateDescriptor<String, String> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }

    @Override
    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
        BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        //将事件名作为key put进广播状态中。后续提取时，对应提取相应事件名的配置。
        JSONObject jsonObject = JSON.parseObject(value);
        JSONObject after = jsonObject.getJSONObject("after");
        if (after != null){
            String event_name = after.getString("event_name");
            String ta_format = after.getString("ta_format");
            broadcastState.put(event_name, ta_format);
        }
    }

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        String ta_conf = "";
        String after_event_name = "";
        //创建JSONObject，存储修改后数据
        JSONObject resultJson = new JSONObject();
        JSONObject property = new JSONObject();

        //获取源数据及映射前事件名
        JSONObject valueJSON = JSON.parseObject(value);
        String before_event_name = valueJSON.getString("_eventname");

        //获取广播流中的配置信息ta_conf，remind事件需要特殊处理
        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        if ("seven_day_remind_back".equals(before_event_name) || "sub_seven_day_remind".equals(before_event_name) || "remind_message_sended".equals(before_event_name)) {
            after_event_name = "remind";
            ta_conf = broadcastState.get(after_event_name);
        } else {
            ta_conf = broadcastState.get(before_event_name);
        }

        //根据获取到的配置信息ta_conf格式化源数据
        if (ta_conf != null) {
            JSONObject controlJSON = JSON.parseObject(ta_conf);
            //获取映射后的字段名集合
            Set<String> afterKeys = controlJSON.keySet();
            //遍历映射后的字段名，按照数数规则处理格式
            for (String afterKey : afterKeys) {
                //获取数数格式信息，包含原字段名、映射后类型、
                JSONArray controlArray = controlJSON.getJSONArray(afterKey);
                if (afterKey.contains("#")) {
                    castFormat(resultJson, valueJSON, afterKey, controlArray, before_event_name, after_event_name);
                } else {
                    castFormat(property, valueJSON, afterKey, controlArray, before_event_name, after_event_name);
                }
            }
            resultJson.put("#type","track");
            resultJson.put("properties", property);
            out.collect(resultJson.toString());
        }
    }


    /**
     * 按数数要求转换格式
     *
     * @param jsonObject        : 存放转换后结果的容器JSON (映射后的JSON)
     * @param valueJSON         : 映射前的JSON，即kafka中的原数据，用于提取值
     * @param afterKey          : 映射后的字段
     * @param controlArray      : 控制数组，包含映射前字段，映射后的类型，对象组类型
     * @param before_event_name : 映射前事件名称
     * @param after_event_name  : 映射后事件名称
     */
    private void castFormat(JSONObject jsonObject, JSONObject valueJSON, String afterKey, JSONArray controlArray, String before_event_name, String after_event_name) {
        //获取映射前字段
        String beforeKey = controlArray.getString(0);
        //获取映射后字段类型
        String afterType = controlArray.getString(1);

        if ("String".equals(afterType)) {
            if ("remind".equals(after_event_name) && "_eventname".equals(beforeKey)) {
                jsonObject.put(afterKey, after_event_name);
            } else {
                //按指定类型提取字段值
                String afterValue = valueJSON.getString(beforeKey);
                //将处理后的k,v输入新的JSON中
                jsonObject.put(afterKey, afterValue);
            }
        } else if ("Number".equals(afterType)) {
            BigInteger afterValue = valueJSON.getBigInteger(beforeKey);
            jsonObject.put(afterKey, afterValue);
        } else if ("Boolean".equals(afterType)) {
            boolean afterValue = valueJSON.getBooleanValue(beforeKey);
            jsonObject.put(afterKey, afterValue);
        } else if ("Zone".equals(afterType)) {
            jsonObject.put(afterKey, 8);
        } else if ("Date".equals(afterType)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date time = valueJSON.getDate(beforeKey);
            Date date = new Date(time.getTime() - 3600 * 8 * 1000);
            String afterValue = sdf.format(date);
            jsonObject.put(afterKey, afterValue);
        } else if ("Array".equals(afterType)) {
            //Array有两种格式，一种以','分隔，一种为JSONarray格式
            String str = valueJSON.getString(beforeKey);
            if (MyUtil.isJSON(str)) {
                JSONObject json = JSON.parseObject(str);
                JSONArray jsonArray = json.getJSONArray(beforeKey);
                ArrayList<String> afterArray = new ArrayList<>();
                for (int i = 0; i < jsonArray.size(); i++) {
                    String str1 = jsonArray.getString(i);
                    String str2 = str1.substring(1, str1.length() - 1);
                    String value = str2.split(":")[1];
                    afterArray.add(value);
                }
                jsonObject.put(afterKey, afterArray);
            } else {
                String substring = str.substring(0, str.length() - 1);
                String[] afterArray = substring.split(",");
                jsonObject.put(afterKey, afterArray);
            }
        } else if ("JSONARRAY".equals(afterType)) {
            String jsonArrayType = controlArray.getString(2);
            if ("0".equals(jsonArrayType)) {
                JSONObject beforeJSON = valueJSON.getJSONObject(beforeKey);
                JSONArray jsonArray = beforeJSON.getJSONArray(beforeKey);
                jsonObject.put(afterKey, jsonArray);
            } else if ("1".equals(jsonArrayType)) {
                //此处的 beforeKey 格式为 {"映射后子列名":["映射前子列名","映射后类型"]}
                JSONObject subControlJson = JSON.parseObject(beforeKey);
                JSONObject subResult = subResultFormat(valueJSON, subControlJson);
                jsonObject.put(afterKey, subResult);
            }
        } else if ("REMINDTYPE".equals(afterType)) {
            jsonObject.put(afterKey, before_event_name);
        }
    }


    /**
     * 对JSONARRAY格式中的子字段进行格式化
     *
     * @param valueJSON      : 映射前的JSON，即kafka中的原数据，用于提取值
     * @param subControlJson : 控制子字段格式的JSON字符串
     * @return 返回处理好格式的子字段JSONObject
     */
    private JSONObject subResultFormat(JSONObject valueJSON, JSONObject subControlJson) {
        //获取映射后子字段名，数组
        Set<String> subAfterKeys = subControlJson.keySet();
        //创建子JSON容器
        JSONObject subResult = new JSONObject();
        //根据子字段的类型获取值
        for (String subAfterKey : subAfterKeys) {
            JSONArray subControlArray = subControlJson.getJSONArray(subAfterKey);
            if ("String".equals(subControlArray.getString(1))) {
                String subBeforeKey = subControlArray.getString(0);
                String subAfterValue = valueJSON.getString(subBeforeKey);
                subResult.put(subAfterKey, subAfterValue);
            } else if ("Number".equals(subControlArray.getString(1))) {
                String subBeforeKey = subControlArray.getString(0);
                BigInteger subAfterValue = valueJSON.getBigInteger(subBeforeKey);
                subResult.put(subAfterKey, subAfterValue);
            } else if ("Boolean".equals(subControlArray.getString(1))) {
                String subBeforeKey = subControlArray.getString(0);
                boolean subAfterValue = valueJSON.getBooleanValue(subBeforeKey);
                subResult.put(subAfterKey, subAfterValue);
            } else if ("CHANGETYPE".equals(subControlArray.getString(1))) {
                BigInteger old_cnt = valueJSON.getBigInteger("old_cnt");
                BigInteger new_cnt = valueJSON.getBigInteger("new_cnt");
                if (new_cnt.compareTo(old_cnt) == 1) {
                    subResult.put(subAfterKey, "增加");
                } else {
                    subResult.put(subAfterKey, "减少");
                }
            }
        }
        return subResult;
    }


}
