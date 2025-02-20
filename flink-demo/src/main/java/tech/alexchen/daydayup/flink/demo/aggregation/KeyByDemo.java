package tech.alexchen.daydayup.flink.demo.aggregation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 09:34
 */
public class KeyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 11L, 11),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 3L, 3)
        );

        // keyBy 是对数据分组，保证相同 key 的数据，在同一个分区；一个分区可能会有多个组
        dataStreamSource.keyBy((KeySelector<WaterSensor, String>) WaterSensor::getId)
                .max("vc")
                .print();

        // maxBy 输出的是，每一条数据进行处理时，vc 值最大的那一条数据;
        // max 则只会在第一条的基础上，变化指定的 vc 字段
        env.execute();
    }
}
