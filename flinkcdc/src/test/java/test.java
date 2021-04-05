import FlinkCDCdemo.TableProcess;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.lang.annotation.Target;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author Mengzc
 * @create 2021-03-28 14:00
 */
public class test {
    @Test
    public void mdkkd() {

        //    TableProcess tableProcess = JSON.parseObject(data.toString(), TableProcess.class);


    }

    @Test
    public void filterColumn1() {
        String a = "{\"sss\":\"\",\"sssr\":\"sss\",\"\":\"dd\"}";
        JSONObject data = JSON.parseObject(a);


        Set<String> keySet = data.keySet();
        Set<Map.Entry<String, Object>> entries = data.entrySet();
     /*     Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
      while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!fieldList.contains(next.getKey())) {
                iterator.remove();
            }
        }
*/
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            String value = (String) next.getValue();
            String key = next.getKey();
            if (StringUtils.isBlank(value) || StringUtils.isBlank(key)) {
                iterator.remove();
            }
        }
        //  entries.removeIf(next -> !keySet.contains(next.getKey()));
        System.out.println(data);
    }
}
