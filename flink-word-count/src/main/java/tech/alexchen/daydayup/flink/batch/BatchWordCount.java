package tech.alexchen.daydayup.flink.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author alexchen
 * @since 2025-02-13 11:45
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> data = new ArrayList<>();
        data.add("hello java");
        data.add("hello python");
        data.add("hello go");
        data.add("hello flink");

        DataStream<Tuple2<String, Integer>> dataStream = env
                .fromData(data)
                .flatMap(new Splitter())
                .keyBy(value -> value.f0)
                .sum(1);
        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
