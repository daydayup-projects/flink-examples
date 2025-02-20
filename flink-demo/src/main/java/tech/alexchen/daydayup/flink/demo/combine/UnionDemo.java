package tech.alexchen.daydayup.flink.demo.combine;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexchen
 * @since 2025-02-20 15:07
 */
public class UnionDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource1 = env.fromData(1, 2, 3);
        DataStreamSource<Integer> streamSource2 = env.fromData(4, 5, 6);
        DataStreamSource<String> streamSource3 = env.fromData("7", "8", "9");

        // union 合并多条流
        DataStream<Integer> unionStream = streamSource1.union(streamSource2)
                .union(streamSource3.map(Integer::valueOf));

        unionStream.print();
        env.execute();
    }
}
