package tech.alexchen.daydayup.flink.demo.watermark;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-25 08:59
 */
public class IntervalJoinWithLateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) {
                        String[] data = value.split(",");
                        return Tuple2.of(data[0], Integer.valueOf(data[1]));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );

        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> ds2 = env
                .socketTextStream("localhost", 8888)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) {
                        String[] data = value.split(",");
                        return Tuple3.of(data[0], Integer.valueOf(data[1]), Integer.valueOf(data[2]));
                    }
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, Integer, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((value, ts) -> value.f1 * 1000L)
                );

        // interval join
        // 1. 分别做 keyBy
        KeyedStream<Tuple2<String, Integer>, String> ks1 = ds1.keyBy(a -> a.f0);
        KeyedStream<Tuple3<String, Integer, Integer>, String> ks2 = ds2.keyBy(a -> a.f0);

        OutputTag<Tuple2<String, Integer>> tag1 = new OutputTag<>("late1", Types.TUPLE(Types.STRING, Types.INT));
        OutputTag<Tuple3<String, Integer, Integer>> tag2 = new OutputTag<>("late2", Types.TUPLE(Types.STRING, Types.INT, Types.INT));

        // 2. 调用 intervalJoin
        SingleOutputStreamOperator<Object> process = ks1.intervalJoin(ks2)
                .between(Duration.ofSeconds(-1), Duration.ofSeconds(1))
                .sideOutputLeftLateData(tag1)
                .sideOutputRightLateData(tag2)
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Object>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, Context ctx, Collector<Object> out) throws Exception {
                        // 两条流的数据匹配上，才会调用该方法
                        out.collect(left + " -----> " + right);
                    }
                });

        process.print();

        process.getSideOutput(tag1).printToErr("ks1 迟到的数据");
        process.getSideOutput(tag2).printToErr("ks2 迟到的数据");
        env.execute();
    }
}
