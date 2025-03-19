package tech.alexchen.daydayup.flink.datastream.watermark;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author alexchen
 * @since 2025-02-24 15:25
 */
public class CustomPeriodWatermarkGenerator<T> implements WatermarkGenerator<T> {

    private final Long delayTime;
    private Long maxTime;

    public CustomPeriodWatermarkGenerator(Long delayTime) {
        this.delayTime = delayTime;
        this.maxTime = Long.MIN_VALUE + this.delayTime + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
        maxTime = Math.max(maxTime, eventTimestamp);
        System.out.println("onEvent, maxTime is " + maxTime + ", watermark is " + (maxTime - delayTime - 1));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        output.emitWatermark(new Watermark(maxTime - delayTime - 1));
    }
}
