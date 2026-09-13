package com.zx.quant.klineproxy.model;

import lombok.Getter;

/**
 * Transport diagnostics carried with a message, never used to order or accept market data.
 * Receive means entry into the decoded, aggregated Netty WebSocket frame callback, not NIC arrival.
 * Durations use nanoTime; the wall clock is retained only for comparison with exchange timestamps.
 * The receiver initializes this object before submission; only the message worker mutates it after.
 */
@Getter
public final class WebSocketMessageTiming {

  private final String client;
  private final long receivedAtMillis;
  private final long receivedAtNanos;
  private long enqueuedAtNanos;
  private long handlerStartedAtNanos;
  private long jsonParsedAtNanos;
  private int queuedAtReceive;
  private int queuedAtStart;
  private int activeThreads;
  private int poolSize;
  private long droppedMessages;

  public WebSocketMessageTiming(String client, long receivedAtMillis, long receivedAtNanos) {
    this.client = client;
    this.receivedAtMillis = receivedAtMillis;
    this.receivedAtNanos = receivedAtNanos;
  }

  public void enqueued(int queued) {
    queuedAtReceive = queued;
    enqueuedAtNanos = System.nanoTime();
  }

  public void handlerStarted(int queued) {
    handlerStartedAtNanos = System.nanoTime();
    queuedAtStart = queued;
  }

  public void jsonParsed() {
    jsonParsedAtNanos = System.nanoTime();
  }

  /** Approximate shared-pool snapshot, sampled only for closing kline messages. */
  public void executorSnapshot(int active, int size, long dropped) {
    activeThreads = active;
    poolSize = size;
    droppedMessages = dropped;
  }
}
