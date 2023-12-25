package com.zx.quant.klineproxy.manager.impl;

import com.google.common.util.concurrent.RateLimiter;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import io.netty.util.HashedWheelTimer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

/**
 * rate limit manager impl
 * @author flamhaze5946
 */
@Service("rateLimitManager")
public class RateLimitManagerImpl implements RateLimitManager, InitializingBean {

  private static final String RATE_LIMIT_TIMER_GROUP = "rate-limit-timer";

  private final Map<String, RateLimiterWrapper> wrapperMap = new ConcurrentHashMap<>();

  private final HashedWheelTimer timer = buildTimer();

  @Override
  public synchronized void registerRateLimiter(String limiterName, long limitPerSecond) {
    if (wrapperMap.containsKey(limiterName)) {
      throw new RuntimeException("rate limiter has already registered.");
    }
    RateLimiter limiter = RateLimiter.create(limitPerSecond);
    RateLimiterWrapper wrapper = new RateLimiterWrapper(limiter);
    wrapperMap.put(limiterName, wrapper);
  }

  @Override
  public void acquire(String limiterName, int weight) {
    RateLimiterWrapper wrapper = getWrapper(limiterName);
    while (wrapper.getStopper().get()) {
      CommonUtil.sleep(50);
    }
    wrapper.getRateLimiter().acquire(weight);
  }

  @Override
  public void stopAcquire(String limiterName, long mills) {
    RateLimiterWrapper wrapper = getWrapper(limiterName);
    wrapper.getStopper().set(true);
    timer.newTimeout(timeout -> wrapper.getStopper().set(false), mills, TimeUnit.MILLISECONDS);
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    registerRateLimiter(Constants.BINANCE_SPOT_KLINES_FETCHER_RATE_LIMITER_NAME, 80);
    registerRateLimiter(Constants.BINANCE_SPOT_GLOBAL_WS_FRAME_RATE_LIMITER_NAME, 1);
    registerRateLimiter(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 35);
    registerRateLimiter(Constants.BINANCE_FUTURE_GLOBAL_WS_FRAME_RATE_LIMITER_NAME, 1);
  }

  private RateLimiterWrapper getWrapper(String limiterName) {
    RateLimiterWrapper wrapper = wrapperMap.get(limiterName);
    if (wrapper == null) {
      throw new RuntimeException("rate limiter not exists.");
    }
    return wrapper;
  }

  private HashedWheelTimer buildTimer() {
    ThreadFactory namedThreadFactory = ThreadFactoryUtil.getNamedThreadFactory(
        RATE_LIMIT_TIMER_GROUP);
    HashedWheelTimer hashedWheelTimer = new HashedWheelTimer(namedThreadFactory, 50,
        TimeUnit.MILLISECONDS);
    hashedWheelTimer.start();
    Runtime.getRuntime().addShutdownHook(new Thread(hashedWheelTimer::stop));
    return hashedWheelTimer;
  }

  @Getter
  private static class RateLimiterWrapper {

    private final RateLimiter rateLimiter;

    private final AtomicBoolean stopper = new AtomicBoolean(false);

    public RateLimiterWrapper(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
    }
  }
}
