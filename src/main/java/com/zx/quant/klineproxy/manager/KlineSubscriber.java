package com.zx.quant.klineproxy.manager;

/**
 * kline subscriber
 * @author flamhaze5946
 */
public interface KlineSubscriber {

  void onMessage(String message);
}
