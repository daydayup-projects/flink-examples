package tech.alexchen.daydayup.flink.datastream.aggregation;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 09:34
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 3L, 3)
        );

        dataStreamSource.keyBy((KeySelector<WaterSensor, String>) WaterSensor::getId)
                .reduce(new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor v1, WaterSensor v2) throws Exception {
                        System.out.println("v1:" + v1);
                        System.out.println("v2:" + v2);
                        return new WaterSensor(v1.getId(), v1.getTs() + v2.getTs(), v1.getVc() + v2.getVc());
                    }
                })
                .print();

        env.execute();
    }
}
