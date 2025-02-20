package tech.alexchen.daydayup.flink.demo.functions;

import lombok.AllArgsConstructor;
import org.apache.flink.api.common.functions.FilterFunction;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 11:16
 */
@AllArgsConstructor
public class WaterSensorFilterFunction implements FilterFunction<WaterSensor> {

    private String id;

    @Override
    public boolean filter(WaterSensor value) throws Exception {
        return value.getId().equals(id);
    }
}
