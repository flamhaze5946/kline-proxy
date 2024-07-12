package com.zx.quant.klineproxy.monitor.impl;

import com.zx.quant.klineproxy.monitor.MonitorManager;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import org.springframework.stereotype.Component;

/**
 * monitor manager implement
 * @author flamhaze5946
 */
@Component
public class MonitorManagerImpl implements MonitorManager {

  private static final String KLINE_COUNTER_NAME = "kline_counter";

  private static final String TICKER_COUNTER_NAME = "ticker_counter";

  private static final String MONITOR_SERVICE_TYPE_LABEL = "service_type";

  private static final String MONITOR_KLINE_INTERVAL_LABEL = "interval";

  private static final String MONITOR_SYMBOL_LABEL = "symbol";

  private final CollectorRegistry registry;

  private final Counter klineCounter;

  private final Counter tickerCounter;

  public MonitorManagerImpl(CollectorRegistry registry) {
    this.registry = registry;
    klineCounter = Counter.build()
        .labelNames(MONITOR_SERVICE_TYPE_LABEL, MONITOR_KLINE_INTERVAL_LABEL, MONITOR_SYMBOL_LABEL)
        .name(KLINE_COUNTER_NAME)
        .help(KLINE_COUNTER_NAME)
        .register(this.registry);
    tickerCounter = Counter.build()
        .labelNames(MONITOR_SERVICE_TYPE_LABEL)
        .name(TICKER_COUNTER_NAME)
        .help(TICKER_COUNTER_NAME)
        .register(this.registry);
  }

  @Override
  public void incReceivedKlineMessage(String serviceType, String interval, String symbol) {
    klineCounter.labels(serviceType, interval, symbol).inc();
  }

  @Override
  public void incReceivedTickerMessage(String serviceType) {
    tickerCounter.labels(serviceType).inc();
  }
}
