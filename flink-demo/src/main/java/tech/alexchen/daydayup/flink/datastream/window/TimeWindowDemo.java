package tech.alexchen.daydayup.flink.datastream.window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorProcessTimeWindowFunction;

/**
 * 窗口的开始时间 = 窗口长度向下取整
 * 窗口的结束时间 = 开始时间 + 窗口长度
 * <p>
 * 窗口的生命周期：
 * 1. 创建：属于本窗口的第一条数据到来时，计算窗口开始结束时间，然后创建单例的窗口对象
 * 2. 销毁：时间 > 窗口结束时间 + 允许的延迟时间
 *
 * @author alexchen
 * @since 2025-02-24 09:02
 */
public class TimeWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> timeWindow;
        // 基于时间的窗口
        // 滚动窗口
//        timeWindow = keyedStream.window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));
        // 滑动窗口，窗口长度 10s，滑动步长2s
//        timeWindow = keyedStream.window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(2)));
        // 会话窗口，超时间隔5s
//        timeWindow = keyedStream.window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)));
        // 会话窗口，动态间隔
        timeWindow = keyedStream.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
            @Override
            public long extract(WaterSensor element) {
                return element.getTs() * 1000L;
            }
        }));

        timeWindow.process(new WaterSensorProcessTimeWindowFunction()).print();
        env.execute();
    }
}
