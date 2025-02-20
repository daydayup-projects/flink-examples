package tech.alexchen.daydayup.flink.demo.combine;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author alexchen
 * @since 2025-02-20 15:07
 */
public class ConnectKeyByDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Tuple2<Integer, String>> source1 = env.fromData(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> source2 = env.fromData(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connect = source1.connect(source2);

        // 多并行度下，需要根据 关联条件 进行 keyBy，才能保证key相同的数据到一起去，才能匹配上
        connect.keyBy(s1 -> s1.f0, s2 -> s2.f0)
                .process(new CustomCoProcessFunction()).print();

        env.execute();
    }

    public static class CustomCoProcessFunction extends CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String> {

        private final Map<Integer, List<Tuple2<Integer, String>>> s1Cache = new HashMap<>();
        private final Map<Integer, List<Tuple3<Integer, String, Integer>>> s2Cache = new HashMap<>();

        @Override
        public void processElement1(Tuple2<Integer, String> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
            Integer id = value.f0;
            if (!s1Cache.containsKey(id)) {
                List<Tuple2<Integer, String>> s1Values = new ArrayList<>();
                s1Values.add(value);
                s1Cache.put(id, s1Values);
            } else {
                s1Cache.get(id).add(value);
            }

            if (s2Cache.containsKey(id)) {
                for (Tuple3<Integer, String, Integer> s2Element : s2Cache.get(id)) {
                    out.collect("s1:" + value + "<--------->s2:" + s2Element);
                }
            }

        }

        @Override
        public void processElement2(Tuple3<Integer, String, Integer> value, CoProcessFunction<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
            Integer id = value.f0;
            if (!s2Cache.containsKey(id)) {
                List<Tuple3<Integer, String, Integer>> s2Values = new ArrayList<>();
                s2Values.add(value);
                s2Cache.put(id, s2Values);
            } else {
                s2Cache.get(id).add(value);
            }

            if (s1Cache.containsKey(id)) {
                for (Tuple2<Integer, String> s1Element : s1Cache.get(id)) {
                    out.collect("s1:" + s1Element + "<--------->s2:" + value);
                }
            }
        }
    }

}
