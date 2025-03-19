package tech.alexchen.daydayup.flink.datastream.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;

/**
 * @author alexchen
 * @since 2025-02-24 10:17
 */
public class KeyedProcessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) {
                        //获取当前数据的key
                        String currentKey = ctx.getCurrentKey();

                        // 1.定时器注册
                        TimerService timerService = ctx.timerService();
                        Long currentEventTime = ctx.timestamp(); // 数据中提取出来的事件时间
                        timerService.registerEventTimeTimer(5000L);

                        // watermark 是上一条数据的 watermark， 因为 watermark 也是一条数据，处理当前数据时，当前数据的 watermark 还没有进入处理函数
                        long watermark = timerService.currentWatermark();
                        System.out.println("当前key=" + currentKey + ",当前时间=" + currentEventTime + ", 上一条的 watermark=" + watermark);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        String currentKey = ctx.getCurrentKey();
                        System.out.println("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
                    }
                })
                .print();

        env.execute();
    }

}
