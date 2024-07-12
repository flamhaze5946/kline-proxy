package com.zx.quant.klineproxy.monitor;

/**
 * monitor manager
 * @author flamhaze5946
 */
public interface MonitorManager {

  /**
   * increase received kline message
   * @param serviceType service type
   * @param interval    interval
   * @param symbol      symbol
   */
  void incReceivedKlineMessage(String serviceType, String interval, String symbol);

  /**
   * increase received ticker message
   * @param serviceType service type
   */
  void incReceivedTickerMessage(String serviceType);
}
