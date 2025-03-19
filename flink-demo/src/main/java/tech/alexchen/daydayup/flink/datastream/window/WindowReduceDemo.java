package tech.alexchen.daydayup.flink.datastream.window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorReduceFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-24 10:17
 */
public class WindowReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);
        WindowedStream<WaterSensor, String, TimeWindow> timeWindow = keyedStream
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)));
        // 归约函数
        timeWindow.reduce(new WaterSensorReduceFunction()).print("TimeWindow result");
        env.execute();
    }
}
