package tech.alexchen.daydayup.flink.datastream.watermark;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.partition.PartitionDemo;

import java.time.Duration;

/**
 * 多并行度下，watermark 以每个分区最小的值为准；
 * 当某个分区没有数据时，可能出现任务的水位线无法推进，就可能导致窗口无法触发
 *
 * @author alexchen
 * @since 2025-02-24 17:10
 */
public class WatermarkIdlenessDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 自定义分区器：数据%分区数，只输入奇数，都只会去往map的一个子任务
        env.socketTextStream("localhost", 9999)
                .partitionCustom(new PartitionDemo.CustomPartitioner(), r -> r)
                .map(Integer::parseInt)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((r, ts) -> r * 1000L)
                                .withIdleness(Duration.ofSeconds(5))  //空闲等待5s
                )
                .keyBy(r -> r % 2)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<Integer, String, Integer, TimeWindow>() {
                    @Override
                    public void process(Integer integer, Context context, Iterable<Integer> elements, Collector<String> out) throws Exception {
                        long startTs = context.window().getStart();
                        long endTs = context.window().getEnd();
                        String windowStart = DateFormatUtils.format(startTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(endTs, "yyyy-MM-dd HH:mm:ss.SSS");
                        long count = elements.spliterator().estimateSize();
                        out.collect("key=" + integer + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                })
                .print();
        env.execute();
    }
}
