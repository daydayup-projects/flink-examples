package tech.alexchen.daydayup.flink.demo.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexchen
 * @since 2025-02-19 15:14
 */
public class CollectionSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        DataStreamSource<Integer> dataStreamSource = env.fromData(1, 2, 3);
        dataStreamSource.print();
        env.execute();
    }
}
