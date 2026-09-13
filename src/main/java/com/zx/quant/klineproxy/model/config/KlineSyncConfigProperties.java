package com.zx.quant.klineproxy.model.config;

import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.IntervalSyncConfig;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kline sync config properties
 * @author flamhaze5946
 */
@Data
public class KlineSyncConfigProperties<C extends IntervalSyncConfig> {

  private Boolean enabled = true;

  private Integer rpcRefreshCount = 99;

  private Map<String, C> intervalSyncConfigs;

  @ConfigurationProperties(prefix = "kline.binance.spot")
  public static class BinanceSpotKlineSyncConfigProperties extends KlineSyncConfigProperties<IntervalSyncConfig> {
  }

  @ConfigurationProperties(prefix = "kline.binance.future")
  public static class BinanceFutureKlineSyncConfigProperties extends KlineSyncConfigProperties<IntervalSyncFutureConfig> {
  }

  @Data
  public static class IntervalSyncConfig {

    private Integer minMaintainCount = 365;

    private List<String> listenSymbolPatterns;
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  public static class IntervalSyncFutureConfig extends IntervalSyncConfig {

    /** Select continuous-contract klines for this interval; false keeps the ordinary symbol stream. */
    private boolean useContinuousKlineStream;
  }
}
