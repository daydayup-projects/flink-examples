package tech.alexchen.daydayup.flink.demo.process;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.StringToWaterSensorMapFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author alexchen
 * @since 2025-02-24 10:17
 */
public class TopNDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((element, recordTimestamp) -> element.getTs() * 1000L)
                )
                .windowAll(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .process(new TopNProcessFunction())
                .print();
        env.execute();
    }

    public static class TopNProcessFunction extends ProcessAllWindowFunction<WaterSensor, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
            Map<Integer, Integer> vcCountMap = new HashMap<>();

            for (WaterSensor element : elements) {
                Integer vc = element.getVc();
                vcCountMap.put(vc, vcCountMap.getOrDefault(vc, 0) + 1);
            }


            List<Tuple2<Integer, Integer>> data = new ArrayList<>();
            for (Integer vc : vcCountMap.keySet()) {
                data.add(Tuple2.of(vc, vcCountMap.get(vc)));
            }
            // 对List进行排序，根据count值 降序
            data.sort(new Comparator<Tuple2<Integer, Integer>>() {
                @Override
                public int compare(Tuple2<Integer, Integer> o1, Tuple2<Integer, Integer> o2) {
                    // 降序， 后 减 前
                    return o2.f1 - o1.f1;
                }
            });

            // 3.取出 count最大的2个 vc
            StringBuilder outStr = new StringBuilder();

            outStr.append("================================\n");
            // 遍历 排序后的 List，取出前2个， 考虑可能List不够2个的情况  ==》 List中元素的个数 和 2 取最小值
            for (int i = 0; i < Math.min(2, data.size()); i++) {
                Tuple2<Integer, Integer> vcCount = data.get(i);
                outStr.append("Top").append(i + 1).append("\n");
                outStr.append("vc=").append(vcCount.f0).append("\n");
                outStr.append("count=").append(vcCount.f1).append("\n");
                outStr.append("窗口结束时间=").append(DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS")).append("\n");
                outStr.append("================================\n");
            }
            out.collect(outStr.toString());
        }
    }

}
