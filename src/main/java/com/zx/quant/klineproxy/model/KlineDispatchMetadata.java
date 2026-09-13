package com.zx.quant.klineproxy.model;

/** Protocol-independent identity, ordering hints and heartbeat topic, available before full decode. */
public record KlineDispatchMetadata(Series series, long openTime, boolean closed, int tradeCount,
    Long eventTime, String topic) {
  public record Series(String market, String symbol, String interval) { }
  public record Bar(Series series, long openTime) { }

  public Bar bar() {
    return new Bar(series, openTime);
  }

  public boolean supersedes(KlineDispatchMetadata previous, long sequence, long previousSequence) {
    if (tradeCount != previous.tradeCount) {
      return tradeCount > previous.tradeCount;
    }
    if (eventTime != null && previous.eventTime != null && !eventTime.equals(previous.eventTime)) {
      return eventTime > previous.eventTime;
    }
    return sequence >= previousSequence;
  }
}
