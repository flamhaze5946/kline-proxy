package com.zx.quant.klineproxy.model.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "kline.ingress")
public class KlineIngressProperties {
  private int workers = 4;
  private int finalQueueCapacityPerWorker = 2048;
  private int formingQueueCapacityPerWorker = 4096;
  private int closedKeyCapacityPerWorker = 2048;
  private boolean coalesceFormingUpdates = true;

  public void validate() {
    if (workers < 1 || workers > 64 || finalQueueCapacityPerWorker < 1
        || formingQueueCapacityPerWorker < 1 || closedKeyCapacityPerWorker < 1) {
      throw new IllegalArgumentException("kline.ingress requires 1..64 workers and positive queue/key capacities");
    }
  }
}
