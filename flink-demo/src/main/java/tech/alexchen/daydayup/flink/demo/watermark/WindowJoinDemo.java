package tech.alexchen.daydayup.flink.demo.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-25 08:59
 */
public class WindowJoinDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromData(
                Tuple2.of("a", 1),
                Tuple2.of("b", 2),
                Tuple2.of("c", 3),
                Tuple2.of("d", 4),
                Tuple2.of("e", 5)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromData(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 12, 2),
                Tuple3.of("b", 6, 3),
                Tuple3.of("c", 4, 4),
                Tuple3.of("d", 5, 5)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        // window join
        // 落在同一个时间窗口，且 keyBy 能匹配上的元素，才能在 JoinFunction 中获取到，类似于 inner join
        ds1.join(ds2)
                .where(a -> a.f0) // ds1 的 keyBy
                .equalTo(b -> b.f0) // ds2 的 keyBy
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
                    @Override
                    public Object join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        // join 里面获取的元素，都是关联上的
                        return first + " ----> " + second;
                    }
                })
                .print();
        env.execute();
    }
}
