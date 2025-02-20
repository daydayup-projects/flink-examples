package tech.alexchen.daydayup.flink.demo.functions;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import tech.alexchen.daydayup.flink.demo.bean.WaterSensor;

/**
 * @author alexchen
 * @since 2025-02-20 11:19
 */
public class WaterSensorRichMapFunction extends RichMapFunction<WaterSensor, String> {

    /**
     * 累加器
     */
    private final IntCounter numLines = new IntCounter();

    @Override
    public String map(WaterSensor value) {
        this.numLines.add(1);
        return value.getId();
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        runtimeContext.addAccumulator("num-lines", this.numLines);

        System.out.println(runtimeContext.getTaskInfo().getTaskName());
        super.open(openContext);
    }

    @Override
    public void close() throws Exception {
        System.out.println("close");
        super.close();
    }
}
