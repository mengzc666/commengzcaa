package 广播流;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Mengzc
 * @create 2021-03-28 10:07
 */
public class broadcastDSfLINK {

    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(broadcastDSfLINK.class);
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        //2.读取流中的数据
        DataStreamSource<String> propertiesStream = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9999);

        //3.定义状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcast = propertiesStream.broadcast(mapStateDescriptor);

        //4.连接数据和广播流
        BroadcastConnectedStream<String, String> connectedStream = dataStream.connect(broadcast);

        //5.处理连接之后的流
        connectedStream.process(new BroadcastProcessFunction<String, String, String>() {

            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                //获取广播状态
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                Iterable<Map.Entry<String, String>> entries = broadcastState.immutableEntries();

                for (Map.Entry<String, String> entry : entries) {
                    String key = entry.getKey();
                    String value1 = entry.getValue();
                    System.out.println("key>>>>"+key);
                    System.out.println("value1>>>>"+value1);
                }
          /*
                for (Map.Entry<Integer, Rule> entry : entries) {
                    final Rule rule = entry.getValue();
                    out.collect(
                            new Keyed<>(
                                    event, KeysExtractor.getKey(rule.getGroupingKeyNames(), event), rule.getRuleId()));
                    ruleCounter++;
                }*/

                String aSwitch = broadcastState.get("Switch1");
                String aSwitch2 = broadcastState.get("Switch2");
                System.out.println("aSwitch2>>>>"+aSwitch2);
                System.out.println("aSwitch2>>>>"+aSwitch);
                if ("1".equals(aSwitch)) {
                    logger.error("\"切换1 \"");
                    String s = value + "111111111111111";
                    out.collect(s);
                } else if ("2".equals(aSwitch)) {
                    logger.error("\"切换2 \"");
                    String s = value + "22222222222222";
                    out.collect(s);
                } else {
                    logger.error("\"切换3 \"");
                    String s = value + "3333333333333";
                    out.collect(s);
                }
            }
            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String[] split = value.split(",");

                //broadcastState.put(split[0],split[1]);
                if (Integer.parseInt(value)==1){
                    broadcastState.put("Switch1", value);
                }else {
                    broadcastState.put("Switch2", value);
                }



            }
        }).print();

        //6.执行任务
        env.execute();

    }


}
