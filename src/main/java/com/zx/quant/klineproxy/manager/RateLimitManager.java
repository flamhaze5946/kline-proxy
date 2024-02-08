package com.zx.quant.klineproxy.manager;


public interface RateLimitManager {
  void registerRateLimiter(String limiterName, int limitPerSecond);

  default void acquire(String limiterName) {
    acquire(limiterName, 1);
  }

  void acquire(String limiterName, int weight);

  void stopAcquire(String limiterName, long mills);
}
