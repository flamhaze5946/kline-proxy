package com.zx.quant.klineproxy.model.config;

import java.util.List;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kline sync config properties
 * @author flamhaze5946
 */
@Data
public class KlineSyncConfigProperties {

  private Boolean enabled = true;

  private Integer rpcRefreshCount = 99;

  private Map<String, IntervalSyncConfig> intervalSyncConfigs;

  @ConfigurationProperties(prefix = "kline.binance.spot")
  public static class BinanceSpotKlineSyncConfigProperties extends KlineSyncConfigProperties {
  }

  @ConfigurationProperties(prefix = "kline.binance.future")
  public static class BinanceFutureKlineSyncConfigProperties extends KlineSyncConfigProperties {
  }

  @Data
  public static class IntervalSyncConfig {

    private Integer minMaintainCount = 1000;

    private List<String> listenSymbolPatterns;
  }
}
