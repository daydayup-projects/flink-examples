package tech.alexchen.daydayup.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author alexchen
 * @since 2025-02-19 11:53
 */
public class FileSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        Path path = new Path("/Users/alexchen/Projects/github/flink-examples/flink-source-demo/src/main/resources/data.txt");
        FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), path).build();
        DataStreamSource<String> dataStreamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "fileSource");
        dataStreamSource.print();
        env.execute();
    }
}
