package tech.alexchen.daydayup.flink.demo.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.StringToWaterSensorMapFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-25 16:56
 */
public class StateTTLDemo {

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

        sensorDS.keyBy(WaterSensor::getId)
                .process(
                        new KeyedProcessFunction<String, WaterSensor, String>() {

                            ValueState<Integer> lastVcState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);

                                StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Duration.ofSeconds(5))
                                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                        .build();

                                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("lastVcState", Types.INT);
                                stateDescriptor.enableTimeToLive(stateTtlConfig); // 设置 ttl
                                lastVcState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                                Integer lastVc = lastVcState.value();
                                out.collect("key=" + value.getId() + ",状态值=" + lastVc);

                                // 如果水位大于10，更新状态值 ===》 写入状态
                                if (value.getVc() > 10) {
                                    lastVcState.update(value.getVc());
                                }
                            }
                        }
                )
                .print();

        env.execute();
    }
}
