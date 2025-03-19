package tech.alexchen.daydayup.flink.datastream.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * @author alexchen
 * @since 2025-02-20 17:27
 */
public class KafkaSinkDemo {

    /**
     * 注意：如果要使用 精准一次 写入Kafka，需要满足以下条件，缺一不可
     * 1、开启checkpoint（后续介绍）
     * 2、设置事务前缀
     * 3、设置事务超时时间： checkpoint间隔 < 事务超时时间 < max的15分钟
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 如果是精准一次，必须开启checkpoint（后续章节介绍）
        env.enableCheckpointing(2000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("10.255.102.40:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.<String>builder()
                        .setTopic("flink-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("flink-")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 10 * 60 * 1000 + "")
                .build();

        dataStreamSource.sinkTo(kafkaSink);
        env.execute();
    }
}
