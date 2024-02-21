package com.zx.quant.klineproxy.model.config;

import com.google.common.collect.Lists;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kline sync config properties
 * @author flamhaze5946
 */
@Data
public class KlineSyncConfigProperties {

  private Boolean enabled = true;

  private Integer minMaintainCount = 1000;

  private Integer rpcRefreshCount = 99;

  private List<String> listenIntervals = Lists.newArrayList(
      IntervalEnum.ONE_HOUR.code()
  );

  private List<String> listenSymbolPatterns = Lists.newArrayList(
      ".*"
  );

  @ConfigurationProperties(prefix = "kline.binance.spot")
  public static class BinanceSpotKlineSyncConfigProperties extends KlineSyncConfigProperties {
  }

  @ConfigurationProperties(prefix = "kline.binance.future")
  public static class BinanceFutureKlineSyncConfigProperties extends KlineSyncConfigProperties {
  }
}
