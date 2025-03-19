package tech.alexchen.daydayup.flink.datastream.state;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;
import tech.alexchen.daydayup.flink.datastream.functions.StringToWaterSensorMapFunction;

import java.time.Duration;

/**
 * @author alexchen
 * @since 2025-02-27 09:11
 */
public class KeyedReducingStateDemo {

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
                .process(new KeyedProcessFunction<String, WaterSensor, Integer>() {
                    private ReducingState<Integer> sumVcState;

                    @Override
                    public void open(Configuration parameters) {
                        sumVcState = this.getRuntimeContext()
                                .getReducingState(new ReducingStateDescriptor<>("sumVcState", Integer::sum, Integer.class));
                    }

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Integer> out) throws Exception {
                        sumVcState.add(value.getVc());
                        out.collect(sumVcState.get());
                    }
                })
                .print();

        env.execute();
    }
}
