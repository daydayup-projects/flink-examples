package tech.alexchen.daydayup.flink.demo.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-20 17:02
 */
public class SinkFileDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每个目录中都会有并行度个数的文件写入
        env.setParallelism(1);

        // 必须开启checkpoint，否则一直都是 .inprogress
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                new MyGeneratorFunction(),
                10000, // 数据总条数
                RateLimiterStrategy.perSecond(1000), // 每秒生成的数据数量
                Types.STRING
        );
        DataStreamSource<String> dataStreamSource = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data-generator");

        FileSink<String> fileSink = FileSink.<String>forRowFormat(new Path("./output"), new SimpleStringEncoder<>())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("flink")
                        .withPartSuffix(".log")
                        .build()
                )
                // 文件滚动策略，1分钟滚动一次 或 大小到达 1kb 滚动一次
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofMillis(1))
                        .withMaxPartSize(new MemorySize(1024))
                        .build())
                .build();
        dataStreamSource.sinkTo(fileSink);
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
