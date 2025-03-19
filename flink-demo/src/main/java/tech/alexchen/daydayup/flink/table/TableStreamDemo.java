package tech.alexchen.daydayup.flink.table;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import tech.alexchen.daydayup.flink.datastream.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-03-18 16:09
 */
public class TableStreamDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        DataStreamSource<WaterSensor> source = env.fromData(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3)
        );

        // 流转表
        Table table = tableEnvironment.fromDataStream(source);
        tableEnvironment.createTemporaryView("sensor", source);
        Table queryTable = tableEnvironment.sqlQuery("select id,ts,vc from sensor where ts > 1");

        // 表转流
        tableEnvironment.toDataStream(queryTable).print();
        // 只要调用了 DataStream api，都需要调用 execute()
        env.execute();
    }
}
