package tech.alexchen.daydayup.flink.datastream.process;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-25 15:52
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((element, ts) -> element.getTs() * 1000L)
                );

        OutputTag<String> warnTag = new OutputTag<>("warn", Types.STRING);
        SingleOutputStreamOperator<WaterSensor> process = sensorDS.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                                // 使用侧输出流告警
                                if (value.getVc() > 10) {
                                    ctx.output(warnTag, "当前水位=" + value.getVc() + ",大于阈值10！！！");
                                }
                                // 主流正常 发送数据
                                out.collect(value);
                            }
                        }
                );

        process.print("主流");
        process.getSideOutput(warnTag).printToErr("warn");
        env.execute();
    }

}
