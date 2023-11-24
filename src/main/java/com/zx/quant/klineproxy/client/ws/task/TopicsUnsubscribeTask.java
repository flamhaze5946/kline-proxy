package com.zx.quant.klineproxy.client.ws.task;

import com.zx.quant.klineproxy.util.queue.SetQueue;
import java.util.List;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * topics unsubscribe task
 * @author flamhaze5946
 */
@Slf4j
public class TopicsUnsubscribeTask extends TopicsSubscribeTask implements Runnable {

  private static final String METHOD = "unsubscribe";


  public TopicsUnsubscribeTask(SetQueue<String> topicsQueue, int maxTopicsPerTime, Consumer<List<String>> unsubscribe0) {
    super(topicsQueue, maxTopicsPerTime, unsubscribe0);
  }

  protected String method() {
    return METHOD;
  }
}
