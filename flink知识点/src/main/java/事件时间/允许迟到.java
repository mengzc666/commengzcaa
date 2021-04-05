package 事件时间;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author Mengzc
 * @create 2021-04-04 21:23
 */
public class 允许迟到 {


    public static void main(String[] args) throws Exception {

        OutputTag<WaterSensor> outputTag = new OutputTag<WaterSensor>("Side") {
        };
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("hadoop102", 9999).map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //3.提取数据中的时间戳字段
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy =
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> waterSensorSOSO = waterSensorDS.
                assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //4.按照id分组

        KeyedStream<WaterSensor, String> keyedStream = waterSensorSOSO.keyBy(WaterSensor::getId);

        //5.开窗,允许迟到数据,侧输出流
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5))).allowedLateness(Time.seconds(4)).sideOutputLateData(outputTag);
        //6.计算总和
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");

        DataStream<WaterSensor> sideOutput = result.getSideOutput(outputTag);


        //7.打印
        result.print();
        sideOutput.print("Side");


        //8.执行任务
        env.execute();


    }
}
