package tech.alexchen.daydayup.flink.demo.checkpoint;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExternalizedCheckpointRetention;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-27 15:34
 */
public class CheckpointConfigDemo {

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        // 最新 指定检查点的存储位置
//        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
//        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///flink/checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        env.setParallelism(1);

        // 检查点配置
        // 代码中用到hdfs，需要导入hadoop依赖、指定访问hdfs的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");
        // 1、启用检查点: 默认是barrier对齐的，周期为5s, 精准一次
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        // 2、指定检查点的存储位置
        checkpointConfig.setCheckpointStorage("file:///Users/alexchen/Projects/github/flink-examples/flink-demo/checkpoints");

        // 3、checkpoint的超时时间: 默认10分钟
        checkpointConfig.setCheckpointTimeout(60000);

        // 4、同时运行中的checkpoint的最大数量
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        // 5、最小等待间隔: 上一轮checkpoint结束 到 下一轮checkpoint开始 之间的间隔，设置了>0,并发就会变成1
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);

        // 6、取消作业时，checkpoint的数据 是否保留在外部系统
        // DELETE_ON_CANCELLATION:主动cancel时，删除存在外部系统的chk-xx目录 （如果是程序突然挂掉，不会删）
        // RETAIN_ON_CANCELLATION:主动cancel时，外部系统的chk-xx目录会保存下来
        checkpointConfig.setExternalizedCheckpointRetention(ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION);

        // 7、允许 checkpoint 连续失败的次数，默认 0--> 表示 checkpoint 一旦失败，job就挂掉
        checkpointConfig.setTolerableCheckpointFailureNumber(1);

        // 开启非对齐检查点的要求： 1. Checkpoint模式必须是精准一次；2. 最大并发必须设为1
        checkpointConfig.enableUnalignedCheckpoints();
        // setAlignedCheckpointTimeout 默认为 0，表示开启非对齐检查点；
        // 如果大于 0，先使用对齐的检查点，开始对齐后，等待对齐时间超过配置的时间，就切换到非对齐检查点
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));

        env.socketTextStream("localhost", 9999)
                .flatMap(
                        (String value, Collector<Tuple2<String, Integer>> out) -> {
                            String[] words = value.split(" ");
                            for (String word : words) {
                                out.collect(Tuple2.of(word, 1));
                            }
                        }
                )
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }

}
