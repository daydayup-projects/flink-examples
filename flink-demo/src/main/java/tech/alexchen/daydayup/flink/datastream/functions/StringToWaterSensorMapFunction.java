package tech.alexchen.daydayup.flink.datastream.functions;

import org.apache.flink.api.common.functions.MapFunction;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-21 14:49
 */
public class StringToWaterSensorMapFunction implements MapFunction<String, WaterSensor> {

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
    }
}
