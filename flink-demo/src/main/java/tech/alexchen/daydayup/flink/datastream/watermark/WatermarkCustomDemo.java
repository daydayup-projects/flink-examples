package tech.alexchen.daydayup.flink.datastream.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorProcessTimeWindowFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-24 15:24
 */
public class WatermarkCustomDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 默认的 watermark 生成间隔是 200ms，可以通过该配置设置
        env.getConfig().setAutoWatermarkInterval(200);

        env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        // 指定自定义的 watermark 生成器
                        WatermarkStrategy.<WaterSensor>forGenerator(context -> new CustomPeriodWatermarkGenerator<>(1000L))
                                .withTimestampAssigner((element, recordTimestamp) -> {
                                    System.out.println("element=" + element);
                                    return element.getTs() * 1000L; // 从输入元素中提取时间戳
                                })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(new WaterSensorProcessTimeWindowFunction())
                .print();
        env.execute();
    }
}
