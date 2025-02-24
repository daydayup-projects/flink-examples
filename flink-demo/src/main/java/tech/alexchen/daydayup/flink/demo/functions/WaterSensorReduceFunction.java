package tech.alexchen.daydayup.flink.demo.functions;

import org.apache.flink.api.common.functions.ReduceFunction;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-24 09:54
 */
public class WaterSensorReduceFunction implements ReduceFunction<WaterSensor> {

    @Override
    public WaterSensor reduce(WaterSensor v1, WaterSensor v2) {
        System.out.println("Execute waterSensorReduceFunction...");
        return new WaterSensor(v1.getId(), v1.getTs() + v2.getTs(), v1.getVc() + v2.getVc());
    }
}
