package tech.alexchen.daydayup.flink.datastream.window;

import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorProcessCountWindowFunction;

/**
 * @author alexchen
 * @since 2025-02-24 09:02
 */
public class CountWindowDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedStream = streamOperator.keyBy(WaterSensor::getId);
        // 计数窗口
        WindowedStream<WaterSensor, String, GlobalWindow> countWindow;
        // 滚动窗口，窗口长度=5个元素
        countWindow = keyedStream.countWindow(5);
        // 滑动窗口，窗口长度=5个元素，滑动步长=2个元素
//        countWindow = keyedStream.countWindow(5, 2);
        // 全局窗口，计数窗口的底层
//        countWindow = keyedStream.window(GlobalWindows.create());

        countWindow.process(new WaterSensorProcessCountWindowFunction()).print();
        env.execute();
    }

}
