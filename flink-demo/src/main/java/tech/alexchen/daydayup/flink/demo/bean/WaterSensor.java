package tech.alexchen.daydayup.flink.demo.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author alexchen
 * @since 2025-02-19 17:32
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WaterSensor {

    private String id;
    private Long ts;
    private Integer vc;

}
