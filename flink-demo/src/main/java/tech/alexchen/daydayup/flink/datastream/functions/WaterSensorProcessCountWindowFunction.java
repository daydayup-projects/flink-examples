package tech.alexchen.daydayup.flink.datastream.functions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-24 10:36
 */
public class WaterSensorProcessCountWindowFunction extends ProcessWindowFunction<WaterSensor, String, String, GlobalWindow> {

    @Override
    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) {
        long timestamp = context.window().maxTimestamp();
        long l = elements.spliterator().estimateSize();
        out.collect("key: " + s + ", size: " + l + ", maxTime: " + timestamp);
    }
}
