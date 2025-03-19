package tech.alexchen.daydayup.flink.datastream.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 09:00
 */
public class WaterSensorFlatMapFunction implements FlatMapFunction<WaterSensor, String> {

    @Override
    public void flatMap(WaterSensor value, Collector<String> out) {
        if ("s1".equals(value.getId())) {
            out.collect(value.getTs().toString());
        } else if ("s2".equals(value.getId())) {
            out.collect(value.getTs().toString());
            out.collect(value.getVc().toString());
        }
    }
}
