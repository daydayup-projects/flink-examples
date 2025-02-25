package tech.alexchen.daydayup.flink.demo.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-25 08:59
 */
public class IntervalJoinDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env.fromData(
                Tuple2.of("a", 5),
                Tuple2.of("b", 6),
                Tuple2.of("c", 7)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env.fromData(
                Tuple3.of("a", 1, 1),
                Tuple3.of("a", 2, 1),
                Tuple3.of("b", 9, 2),
                Tuple3.of("b", 10, 2),
                Tuple3.of("c", 6, 3)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                        .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
        );

        // interval join
        // 1. 分别做 keyBy
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(a -> a.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(a -> a.f0);

        // 2. 调用 intervalJoin
        ks1.intervalJoin(ks2)
                .between(Duration.ofSeconds(-3), Duration.ofSeconds(3))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<Object> out) throws Exception {
                        // 两条流的数据匹配上，才会调用该方法
                        out.collect(left + " -----> " + right);
                    }
                })
                .print();
        env.execute();
    }
}
