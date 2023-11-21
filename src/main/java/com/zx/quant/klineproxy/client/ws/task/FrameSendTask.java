package com.zx.quant.klineproxy.client.ws.task;

import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * topics unsubscribe task
 * @author flamhaze5946
 */
@Slf4j
public class FrameSendTask implements Runnable {

  private final Queue<WebSocketFrame> frameQueue;

  private final int maxFramesPerTime;

  private final Consumer<List<WebSocketFrame>> sendConsumer;

  public FrameSendTask(Queue<WebSocketFrame> frameQueue, int maxFramesPerTime, Consumer<List<WebSocketFrame>> sendConsumer) {
    this.frameQueue = frameQueue;
    this.maxFramesPerTime = maxFramesPerTime;
    this.sendConsumer = sendConsumer;
  }

  @Override
  public void run() {
  }
}
