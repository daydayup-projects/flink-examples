package tech.alexchen.daydayup.flink.demo.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.WaterSensorFlatMapFunction;

/**
 * 一进多出或者零出
 *
 * @author alexchen
 * @since 2025-02-19 17:31
 */
public class FlatMapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );
        dataStreamSource.flatMap(new WaterSensorFlatMapFunction()).print();
        env.execute();
    }

}
