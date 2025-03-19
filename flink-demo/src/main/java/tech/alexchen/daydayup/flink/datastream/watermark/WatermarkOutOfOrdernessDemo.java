package tech.alexchen.daydayup.flink.datastream.watermark;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorProcessTimeWindowFunction;

import java.time.Duration;

/**
 * 无序流：
 * watermark = 当前最大的事件事件 - 延迟时间 - 1ms
 *
 * @author alexchen
 * @since 2025-02-24 10:17
 */
public class WatermarkOutOfOrdernessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        // 升序的 watermark，没有等待时间
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> {
                                    System.out.println("element=" + element);
                                    return element.getTs() * 1000L; // 从输入元素中提取时间戳
                                })
                )
                .keyBy(WaterSensor::getId)
                // 需要修改成，使用事件时间的窗口
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .process(new WaterSensorProcessTimeWindowFunction())
                .print();

        env.execute();
    }

}
