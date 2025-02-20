package tech.alexchen.daydayup.flink.demo.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexchen
 * @since 2025-02-19 17:01
 */
public class DataGeneratorSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 总数量会按并行度均分
        env.setParallelism(2);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new MyGeneratorFunction(),
                100, // 数据总条数
                RateLimiterStrategy.perSecond(1), // 每秒生成的数据数量
                Types.STRING
        );
        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");
        dataStreamSource.print();

        env.execute();
    }

    public static class MyGeneratorFunction implements GeneratorFunction<Long, String> {
        public MyGeneratorFunction() {
        }

        @Override
        public String map(Long value) {
            return "Number:" + value;
        }
    }
}
