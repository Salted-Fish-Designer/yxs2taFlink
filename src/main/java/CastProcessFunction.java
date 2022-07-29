import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import func.MyUtil;
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
        String event_name = after.getString("event_name");
        String ta_format = after.getString("ta_format");
        broadcastState.put(event_name, ta_format);
    }

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        //将kafka中的数据的事件名
        JSONObject valueJSON = JSON.parseObject(value);
        String event_name = valueJSON.getString("_eventname");

        //获取广播流中的配置
        ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String ta_conf = broadcastState.get(event_name);
        JSONObject controlJSON = JSON.parseObject(ta_conf);

        //建立一个新的JSONObject，存储修改后信息
        JSONObject resultJson = new JSONObject();
        JSONObject property = new JSONObject();

        if (ta_conf != null) {
            //获取映射后的字段名集合
            Set<String> afterKeys = controlJSON.keySet();
            //遍历映射后的字段名，按照数数规则处理格式
            for (String afterKey : afterKeys) {
                //获取数数格式信息，包含原字段名、映射后类型、
                JSONArray controlArray = controlJSON.getJSONArray(afterKey);

                if (afterKey.contains("#")){
                    castFormat(resultJson,valueJSON,afterKey,controlArray);
                } else {
                    castFormat(property,valueJSON,afterKey,controlArray);
                }
            }
            resultJson.put("properties",property);
        } else {
            System.out.println(event_name + "不存在！");
        }

        out.collect(resultJson.toString());
    }


    /**
     * 按数数要求转换格式
     * @param jsonObject : 存放转换后结果的容器JSON (映射后的JSON)
     * @param valueJSON : 映射前的JSON
     * @param afterKey : 映射后的字段
     * @param controlArray : 控制数组，包含映射前字段，映射后的类型，对象组类型
     */
    private void castFormat(JSONObject jsonObject, JSONObject valueJSON, String afterKey, JSONArray controlArray){
        //获取映射前字段
        String beforeKey = controlArray.getString(0);
        //获取映射后字段类型
        String afterType = controlArray.getString(1);

        if ("String".equals(afterType)){
            //按指定类型提取字段值
            String afterValue = valueJSON.getString(beforeKey);
            //将处理后的k,v输入新的JSON中
            jsonObject.put(afterKey, afterValue);
        }else if ("Number".equals(afterType)){
            BigInteger afterValue = valueJSON.getBigInteger(beforeKey);
            jsonObject.put(afterKey, afterValue);
        }else if ("Boolean".equals(afterType)){
            boolean afterValue = valueJSON.getBooleanValue(beforeKey);
            jsonObject.put(afterKey,afterValue);
        }else if ("Zone".equals(afterType)){
            jsonObject.put(afterKey, 8);
        }else if ("Date".equals(afterType)){
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Date time = valueJSON.getDate(beforeKey);
            String afterValue = sdf.format(time);
            jsonObject.put(afterKey, afterValue);
        }else if ("Array".equals(afterType)){
            //Array有两种格式，一种以','分隔，一种为JSONarray格式
            String str = valueJSON.getString(beforeKey);
            if (MyUtil.isJSON(str)){
                JSONObject json = JSON.parseObject(str);
                JSONArray jsonArray = json.getJSONArray(beforeKey);
                ArrayList<String> afterArray = new ArrayList<>();
                for (int i = 0; i < jsonArray.size(); i++) {
                    String str1 = jsonArray.getString(i);
                    String str2 = str1.substring(1, str1.length() - 1);
                    String value = str2.split(":")[1];
                    afterArray.add(value);
                }
                jsonObject.put(afterKey,afterArray);
            }else {
                String substring = str.substring(0, str.length() - 1);
                String[] afterArray = substring.split(",");
                jsonObject.put(afterKey,afterArray);
            }
        }else if ("JSONARRAY".equals(afterType)){
            String jsonArrayType = controlArray.getString(2);
            if ("0".equals(jsonArrayType)){
                JSONObject beforeJSON = valueJSON.getJSONObject(beforeKey);
                JSONArray jsonArray = beforeJSON.getJSONArray(beforeKey);
                jsonObject.put(afterKey,jsonArray);
            } else if ("1".equals(jsonArrayType)){
                //此处的 beforeKey 为 {"映射后子列名":["映射前子列名","映射后类型"]}
                JSONObject subControlJson = JSON.parseObject(beforeKey);
                //获取映射后子字段名，数组
                Set<String> subAfterKeys = subControlJson.keySet();
                //创建子JSON容器
                JSONObject subResult = new JSONObject();
                //根据子字段的类型获取值
                for (String subAfterKey : subAfterKeys) {
                    JSONArray subControlArray = subControlJson.getJSONArray(subAfterKey);
                    if ("String".equals(subControlArray.getString(1))){
                        String subBeforeKey = subControlArray.getString(0);
                        String subAfterValue = valueJSON.getString(subBeforeKey);
                        subResult.put(subAfterKey, subAfterValue);
                    } else if ("Number".equals(subControlArray.getString(1))){
                        String subBeforeKey = subControlArray.getString(0);
                        BigInteger subAfterValue = valueJSON.getBigInteger(subBeforeKey);
                        subResult.put(subAfterKey, subAfterValue);
                    } else if ("Boolean".equals(subControlArray.getString(1))){
                        String subBeforeKey = subControlArray.getString(0);
                        boolean subAfterValue = valueJSON.getBooleanValue(subBeforeKey);
                        subResult.put(subAfterKey, subAfterValue);
                    } else if ("CHANGETYPE".equals(subControlArray.getString(1))){
                        BigInteger old_cnt = valueJSON.getBigInteger("old_cnt");
                        BigInteger new_cnt = valueJSON.getBigInteger("new_cnt");
                        if (new_cnt.compareTo(old_cnt)==1){
                            subResult.put(subAfterKey,"增加");
                        }else {
                            subResult.put(subAfterKey,"减少");
                        }
                    }
                }
                jsonObject.put(afterKey,subResult);

            }
        }
    }


}
