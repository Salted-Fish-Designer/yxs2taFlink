package func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class MyUtil {
    /**
     * 判断字符串是否为正确的JSON格式
     * @param str
     * @return
     */
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
