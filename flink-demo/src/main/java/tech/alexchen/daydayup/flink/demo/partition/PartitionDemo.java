package tech.alexchen.daydayup.flink.demo.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 分区
 *
 * @author alexchen
 * @since 2025-02-20 11:32
 */
public class PartitionDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("localhost", 9999);

        // 随机分区
//        dataStreamSource.shuffle().print();
        // 轮训分区
//        dataStreamSource.rebalance().print();
        // 重缩放分区，局部组队
//        dataStreamSource.rescale().print();
        // 发送给下游所有任务
//        dataStreamSource.broadcast().print();
        // 只发往第一个子任务
//        dataStreamSource.global().print();
        // 相同 key 的发往同一个子任务
//        dataStreamSource.keyBy(new CustomKeySelector()).print();
        // 自定义分区
        dataStreamSource.partitionCustom(new CustomPartitioner(), new CustomKeySelector()).print();
        env.execute();
    }

    public static class CustomPartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            return Integer.parseInt(key) % numPartitions;
        }
    }

    public static class CustomKeySelector implements KeySelector<String, String>  {
        @Override
        public String getKey(String s) {
            return s;
        }
    }

}
