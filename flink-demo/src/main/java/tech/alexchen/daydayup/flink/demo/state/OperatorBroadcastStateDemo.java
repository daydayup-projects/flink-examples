package tech.alexchen.daydayup.flink.demo.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.StringToWaterSensorMapFunction;

/**
 * @author alexchen
 * @since 2025-02-27 10:14
 */
public class OperatorBroadcastStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 数据流
        SingleOutputStreamOperator<WaterSensor> dataStream = env
                .socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        // 配置流
        DataStreamSource<String> configDataStream = env.socketTextStream("localhost", 8888);

        // 广播状态
        MapStateDescriptor<String, Integer> broadcastMapState = new MapStateDescriptor<>("broadcast-state", Types.STRING, Types.INT);
        // 将配置流广播
        BroadcastStream<String> configBroadcastStream = configDataStream.broadcast(broadcastMapState);

        // 连接两个流
        BroadcastConnectedStream<WaterSensor, String> connectedStream = dataStream.connect(configBroadcastStream);

        connectedStream.process(new BroadcastProcessFunction<WaterSensor, String, String>() {

            /**
             * 数据流处理方法
             */
            @Override
            public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 通过上下文获取广播状态，取出里面的值（只读，不能修改）
                ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                Integer threshold = broadcastState.get("threshold");

                // 判断广播状态里是否有数据，因为刚启动时，可能是数据流的第一条数据先来
                threshold = (threshold == null ? 0 : threshold);

                if (value.getVc() > threshold) {
                    out.collect(value.getId() + " 的水位为 " + value.getVc() + ", 超过了指定的阈值：" + threshold);
                }
            }

            /**
             * 配置流处理方法
             */
            @Override
            public void processBroadcastElement(String value, BroadcastProcessFunction<WaterSensor, String, String>.Context ctx, Collector<String> out) throws Exception {
                // 通过上下文获取广播状态，往里面写数据
                BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(broadcastMapState);
                broadcastState.put("threshold", Integer.valueOf(value));
            }

        }).print();

        env.execute();
    }
}
