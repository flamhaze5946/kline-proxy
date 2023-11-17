package com.zx.quant.klineproxy.manager.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.service.impl.BinanceFutureKlineServiceImpl;
import com.zx.quant.klineproxy.service.impl.BinanceSpotKlineServiceImpl;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

/**
 * rate limit manager impl
 * @author flamhaze5946
 */
@Service("rateLimitManager")
public class RateLimitManagerImpl implements RateLimitManager, InitializingBean {

  private final Map<String, RateLimiter> limiterMap = new ConcurrentHashMap<>();

  @Override
  public synchronized void registerRateLimiter(String limiterName, long limitPerSecond) {
    if (limiterMap.containsKey(limiterName)) {
      throw new RuntimeException("rate limiter has already registered.");
    }
    RateLimiter limiter = RateLimiter.create(limitPerSecond);
    limiterMap.put(limiterName, limiter);
  }

  @Override
  public void acquire(String limiterName, int weight) {
    RateLimiter limiter = limiterMap.get(limiterName);
    if (limiter == null) {
      throw new RuntimeException("rate limiter not exists.");
    }
    limiter.acquire(weight);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    registerRateLimiter(BinanceFutureKlineServiceImpl.class.getName(), 60);
    registerRateLimiter(BinanceSpotKlineServiceImpl.class.getName(), 60);
  }
}
