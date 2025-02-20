package tech.alexchen.daydayup.flink.demo.functions;

import org.apache.flink.api.common.functions.MapFunction;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 08:55
 */
public class WaterSensorMapFunction implements MapFunction<WaterSensor, String> {

    @Override
    public String map(WaterSensor value) {
        return value.getId();
    }
}
