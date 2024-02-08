package com.zx.quant.klineproxy.manager.impl;

import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.util.CommonUtil;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import io.github.resilience4j.core.functions.CheckedRunnable;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.netty.util.HashedWheelTimer;
import java.time.Duration;
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

  private static final CheckedRunnable DO_NOTHING = () -> {};

  private static final String RATE_LIMIT_TIMER_GROUP = "rate-limit-timer";

  private final Map<String, RateLimiterWrapper> wrapperMap = new ConcurrentHashMap<>();

  private final HashedWheelTimer timer = buildTimer();

  @Override
  public synchronized void registerRateLimiter(String limiterName, int limitPerSecond) {
    if (wrapperMap.containsKey(limiterName)) {
      throw new RuntimeException("rate limiter has already registered.");
    }
    RateLimiterConfig config = RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .limitForPeriod(limitPerSecond)
        .timeoutDuration(Duration.ofHours(1))
        .build();
    RateLimiterRegistry registry = RateLimiterRegistry.of(config);
    RateLimiter limiter = registry.rateLimiter(limiterName);
    RateLimiterWrapper wrapper = new RateLimiterWrapper(limiter);
    wrapperMap.put(limiterName, wrapper);
  }

  @Override
  public void acquire(String limiterName, int weight) {
    RateLimiterWrapper wrapper = getWrapper(limiterName);
    while (wrapper.getStopper().get()) {
      CommonUtil.sleep(50);
    }
    CheckedRunnable checkedRunnable = RateLimiter.decorateCheckedRunnable(wrapper.getRateLimiter(), weight, DO_NOTHING);
    checkedRunnable.unchecked().run();
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
