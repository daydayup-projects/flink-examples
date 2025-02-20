package tech.alexchen.daydayup.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexchen
 * @since 2025-02-19 15:44
 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("10.255.102.40:9092")
                .setGroupId("flink-group")
                .setTopics("flink-topic")
                .setValueOnlyDeserializer(new SimpleStringSchema()) // 值序列化器
                .setStartingOffsets(OffsetsInitializer.earliest()) // 消费 kafka 的偏移量策略
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        dataStreamSource.print();

        env.execute();
    }
}
