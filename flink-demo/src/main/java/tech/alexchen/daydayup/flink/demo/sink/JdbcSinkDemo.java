package tech.alexchen.daydayup.flink.demo.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.datasource.statements.JdbcQueryStatement;
import org.apache.flink.connector.jdbc.sink.JdbcSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;
import tech.alexchen.daydayup.flink.demo.functions.StringToWaterSensorMapFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * @author alexchen
 * @since 2025-02-20 17:27
 */
public class JdbcSinkDemo {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> dataSource = env.socketTextStream("localhost", 9999)
                .map(new StringToWaterSensorMapFunction());

        // 旧写法
//        SinkFunction<WaterSensor> sink = JdbcSink.sink("insert into ws values(?, ?, ?)",
//                (JdbcStatementBuilder<WaterSensor>) (ps, t) -> {
//                    ps.setString(1, t.getId());
//                    ps.setLong(2, t.getTs());
//                    ps.setInt(3, t.getVc());
//                },
//                JdbcExecutionOptions.builder()
//                        .withMaxRetries(3) // 重试次数
//                        .withBatchSize(100) // 批次的大小：条数
//                        .withBatchIntervalMs(3000) // 批次的时间
//                        .build(),
//                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                        .withUrl("jdbc:mysql://47.106.253.235:3306/zeus_upms?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
//                        .withUsername("zeus")
//                        .withPassword("1qaz@wsx?S")
//                        .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
//                        .build()
//        );
//        dataSource.addSink(sink);


        // 新写法
        JdbcSink<WaterSensor> sink = JdbcSink.<WaterSensor>builder()
                .withQueryStatement(new JdbcQueryStatement<WaterSensor>() {
                    @Override
                    public String query() {
                        return "insert into ws values(?, ?, ?)";
                    }

                    @Override
                    public void statement(PreparedStatement ps, WaterSensor waterSensor) throws SQLException {
                        ps.setString(1, waterSensor.getId());
                        ps.setLong(2, waterSensor.getTs());
                        ps.setInt(3, waterSensor.getVc());
                    }
                })
                .withExecutionOptions(JdbcExecutionOptions.builder()
                        .withMaxRetries(3) // 重试次数
                        .withBatchSize(100) // 批次的大小：条数
                        .withBatchIntervalMs(3000) // 批次的时间
                        .build())
                .buildAtLeastOnce(
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://47.106.253.235:3306/zeus_upms?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                                .withUsername("zeus")
                                .withPassword("1qaz@wsx?S")
                                .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                                .build());
        dataSource.sinkTo(sink);
        env.execute();
    }


}
