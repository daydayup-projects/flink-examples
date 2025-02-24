package tech.alexchen.daydayup.flink.demo.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-24 10:07
 */
public class WaterSensorAggregateFunction implements AggregateFunction<WaterSensor, Integer, String> {

    @Override
    public Integer createAccumulator() {
        System.out.println("createAccumulator");
        return 0;
    }

    @Override
    public Integer add(WaterSensor waterSensor, Integer acc) {
        System.out.println("add...");
        return acc + waterSensor.getVc();
    }

    @Override
    public String getResult(Integer acc) {
        System.out.println("getResult");
        return acc.toString();
    }

    /**
     * 只有会话窗口会用到
     *
     * @param acc1 累加器1
     * @param acc2 累加器2
     * @return 合并后的累加器
     */
    @Override
    public Integer merge(Integer acc1, Integer acc2) {
        return null;
    }
}
