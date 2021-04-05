package dynamicKeyFunction;

import com.mengzc.utils.MyKafkaUtil;
import dynamicKeyFunction.Rule.*;
import dynamicKeyFunction.bean.Keyed;
import dynamicKeyFunction.func.DynamicKeyFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

/**
 * @author Mengzc
 * @create 2021-03-28 19:32
 */
public class Testmain {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//事件时间语义
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        ck相关
        env.getCheckpointConfig().setCheckpointInterval(9000L);
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(10000L);
        env.enableCheckpointing(10000);
        //flink source 规则流
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource("test", "test");
        // Streams setup
        SingleOutputStreamOperator<String> ddd = env.addSource(cartInfoSource).name("ddd").setParallelism(1);
        DataStream<Rule> rulesUpdateStream = stringsStreamToRules(ddd);

        //主流
        FlinkKafkaConsumer<String> transactionSource = MyKafkaUtil.getKafkaSource("test1", "test1");

        DataStream<String> transactionsStringsStream = env.addSource(transactionSource).name("Transactions Source").setParallelism(1);
        DataStream<Transaction> transactions = stringsStreamToTransactions(transactionsStringsStream);


        //广播流对规则进行广播
        BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);
        //将规则流和广播流进行connect
        BroadcastConnectedStream<Transaction, Rule> connect = transactions.connect(rulesStream);
        SingleOutputStreamOperator<Keyed<Transaction, String, Integer>> name = connect.process(new DynamicKeyFunction()).uid("ddc").name("aaa");
        //分流
        name.keyBy(new KeySelector<Keyed<Transaction, String, Integer>, String>() {
            @Override
            public String getKey(Keyed<Transaction, String, Integer> value) throws Exception {
                return value.getKey();
            }
        });



    }

    //将String 转化为 RULE
    public static DataStream<Rule> stringsStreamToRules(DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())
                .name("Rule Deserialization")
                .setParallelism(1)
                .assignTimestampsAndWatermarks(
                        new BoundedOutOfOrdernessTimestampExtractor<Rule>(Time.of(0, TimeUnit.MILLISECONDS)) {
                            @Override
                            public long extractTimestamp(Rule element) {
                                // Prevents connected data+update stream watermark stalling.
                                return Long.MAX_VALUE;
                            }
                        });
    }



    public static DataStream<Transaction> stringsStreamToTransactions(
            DataStream<String> transactionStrings) {
        return transactionStrings
                .flatMap(new JsonDeserializer<Transaction>(Transaction.class))
                .returns(Transaction.class)
                .flatMap(new TimeStamper<Transaction>())
                .returns(Transaction.class)
                .name("Transactions Deserialization");
    }

    public static class Descriptors {
        public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

        public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {
        };
        public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
        };
        public static final OutputTag<Rule> currentRulesSinkTag =
                new OutputTag<Rule>("current-rules-sink") {
                };
    }
}
