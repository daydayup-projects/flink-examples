package tech.alexchen.daydayup.flink.datastream.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.WaterSensorFilterFunction;

/**
 * @author alexchen
 * @since 2025-02-19 17:31
 */
public class FilterDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 3L, 3)
        );
        dataStreamSource.filter(new WaterSensorFilterFunction("s1")).print();
        env.execute();
    }

}
