package tech.alexchen.daydayup.flink.datastream.combine;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author alexchen
 * @since 2025-02-20 15:07
 */
public class ConnectDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource1 = env.fromData(1, 2, 3);
        DataStreamSource<String> streamSource2 = env.fromData("a", "b", "c");

        // 一条流一次只能连接一条其他的流
        ConnectedStreams<Integer, String> connectedStream = streamSource1.connect(streamSource2);
        SingleOutputStreamOperator<String> result = connectedStream.map(new CustomMapFunction());

        result.print();

        env.execute();
    }

    public static class CustomMapFunction implements CoMapFunction<Integer, String, String> {
        @Override
        public String map1(Integer value) throws Exception {
            return value.toString();
        }

        @Override
        public String map2(String value) throws Exception {
            return value;
        }
    }

}
