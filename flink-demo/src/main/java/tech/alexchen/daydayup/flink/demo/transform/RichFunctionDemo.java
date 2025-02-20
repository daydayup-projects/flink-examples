package tech.alexchen.daydayup.flink.demo.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.WaterSensorRichMapFunction;

/**
 * @author alexchen
 * @since 2025-02-20 11:19
 */
public class RichFunctionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> dataStreamSource = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s2", 3L, 3)
        );
        dataStreamSource.map(new WaterSensorRichMapFunction()).print();
        Object accumulatorResult = env.execute().getAccumulatorResult("num-lines");
        System.out.println("sum:" + accumulatorResult);
    }
}
