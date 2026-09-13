package com.zx.quant.klineproxy.service.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import java.util.Locale;
import org.apache.commons.lang3.StringUtils;

/** Ordinary symbol klines, used by both futures and spot. */
public final class BinanceKlineStream extends AbstractBinanceKlineStream {

  @Override
  public String eventType() {
    return "kline";
  }

  @Override
  public String subscriptionTopic(String symbol, String interval) {
    return symbol.toLowerCase(Locale.ROOT) + "@kline_" + interval;
  }

  @Override
  protected String resolveSymbol(EventKlineEvent<?, ?> event) {
    return event.getSymbol();
  }

  @Override
  protected String resolveSymbol(BinanceKlineHeader header) {
    return header.symbol();
  }

  @Override
  protected String rawTopic(BinanceKlineHeader header) {
    return StringUtils.isAnyBlank(header.symbol(), header.interval()) ? null
        : subscriptionTopic(header.symbol(), header.interval());
  }

  @Override
  protected String rawTopic(JsonNode payload) {
    String symbol = text(payload, "s");
    String interval = interval(payload);
    return StringUtils.isAnyBlank(symbol, interval) ? null : subscriptionTopic(symbol, interval);
  }
}
