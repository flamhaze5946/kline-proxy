package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.WebSocketFrameWrapper;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * topics unsubscribe task
 * @author flamhaze5946
 */
@Slf4j
public class FrameSendTask implements Runnable {

  private final Consumer<WebSocketFrame> frameConsumer;

  private final Queue<WebSocketFrameWrapper> frameWrappers;

  private final ExecutorService executor;


  public FrameSendTask(Consumer<WebSocketFrame> frameConsumer, Queue<WebSocketFrameWrapper> frameWrappers, ExecutorService executor) {
    this.frameConsumer = frameConsumer;
    this.frameWrappers = frameWrappers;
    this.executor = executor;
  }

  @Override
  public void run() {
    WebSocketFrameWrapper wrapper = frameWrappers.poll();
    if (wrapper == null) {
      return;
    }
    WebSocketFrame frame = wrapper.frame();
    Runnable afterSendFunc = wrapper.afterSendFunc();
    frameConsumer.accept(frame);
    if (afterSendFunc != null) {
      CompletableFuture.runAsync(afterSendFunc, executor);
    }
  }
}
