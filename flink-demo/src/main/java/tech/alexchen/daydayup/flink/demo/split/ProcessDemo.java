package tech.alexchen.daydayup.flink.demo.split;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * process 分流
 *
 * @author alexchen
 * @since 2025-02-20 14:42
 */
public class ProcessDemo {

    public static OutputTag<WaterSensor> outputTagS1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class));
    public static OutputTag<WaterSensor> outputTagS2 = new OutputTag<WaterSensor>("s2", Types.POJO(WaterSensor.class));


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> streamOperator = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                });

        // process 返回的是主流的数据
        SingleOutputStreamOperator<WaterSensor> process = streamOperator.process(new CustomProcess());

        OutputTag<WaterSensor> tag1 = new OutputTag<WaterSensor>("s1", Types.POJO(WaterSensor.class));
        SideOutputDataStream<WaterSensor> s1 = process.getSideOutput(tag1);
        SideOutputDataStream<WaterSensor> s2 = process.getSideOutput(outputTagS2);

        s1.print("s1");
        s2.print("s2");

        process.print("主流：");

        env.execute();
    }

    public static class CustomProcess extends ProcessFunction<WaterSensor, WaterSensor> {

        @Override
        public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
            String id = value.getId();
            if ("s1".equals(id)) {
                ctx.output(outputTagS1, value);
            } else if ("s2".equals(id)) {
                ctx.output(outputTagS2, value);
            } else {
                // 其他数据放入主流
                out.collect(value);
            }
        }
    }
}
