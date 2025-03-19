package tech.alexchen.daydayup.flink.datastream.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorAggregateFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-24 10:17
 */
public class WindowAggregateAndProcessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> timeWindow = keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));

        // 可以先聚合，在使用全窗口函数
        // 经过聚合后，全窗口函数拿到的是一条结果数据了，所有里面的 elements 只会有一条
        timeWindow.aggregate(new WaterSensorAggregateFunction(), new StringProcessWindowFunction()).print();
        env.execute();
    }


    public static class StringProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<String> elements, Collector<String> out) {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
            String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

            long l = elements.spliterator().estimateSize();

            out.collect("key: " + s + ", size: " + l + ", startTime: " + startTime + ", endTime: " + endTime);
        }
    }

}
