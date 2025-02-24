package tech.alexchen.daydayup.flink.demo.bean;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * @author alexchen
 * @since 2025-02-19 17:32
 */
@Getter
@Setter
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    private String id;
    private Long ts;
    private Integer vc;

    @Override
    public String toString() {
        return id + "," + ts + "," + vc;
    }
}
