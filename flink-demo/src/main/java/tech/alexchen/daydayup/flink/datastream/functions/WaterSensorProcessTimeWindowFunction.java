package tech.alexchen.daydayup.flink.datastream.functions;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-24 10:36
 */
public class WaterSensorProcessTimeWindowFunction extends ProcessWindowFunction<WaterSensor, String, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) {
        long start = context.window().getStart();
        long end = context.window().getEnd();
        String startTime = DateFormatUtils.format(start, "yyyy-MM-dd HH:mm:ss");
        String endTime = DateFormatUtils.format(end, "yyyy-MM-dd HH:mm:ss");

        long l = elements.spliterator().estimateSize();

        out.collect("key: " + s + ", size: " + l + ", startTime: " + startTime + ", endTime: " + endTime);
    }
}
