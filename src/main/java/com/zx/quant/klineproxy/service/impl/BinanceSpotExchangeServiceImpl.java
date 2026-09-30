package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.util.BlockingWorkExecutor;
import jakarta.annotation.PreDestroy;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.client.model.BinanceServerTime;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotSymbol;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.cache.TradingSymbolsCache;
import java.util.Set;
import com.zx.quant.klineproxy.util.ClientUtil;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * binance spot exchange service impl
 * @author flamhaze5946
 */
@Service("binanceSpotExchangeService")
public class BinanceSpotExchangeServiceImpl implements ExchangeService<BinanceSpotExchange>, InitializingBean {

  private final BlockingWorkExecutor cacheLoads = new BlockingWorkExecutor("spot-metadata-loader", 2, 32);

  @PreDestroy
  public void closeCacheLoads() {
    cacheLoads.close();
  }

  private static final String VALID_SYMBOL_STATUS = "TRADING";

  private static final String SERVER_TIME_REFRESHER_GROUP = "spotServerTimeRefresher";

  private final ScheduledExecutorService serverTimeRefresher = new ScheduledThreadPoolExecutor(1,
      ThreadFactoryUtil.getNamedThreadFactory(SERVER_TIME_REFRESHER_GROUP));

  private final LoadingCache<String, BinanceSpotExchange> exchangeCache = buildExchangeCache();

  private final TradingSymbolsCache<BinanceSpotExchange> tradingSymbols = new TradingSymbolsCache<>(
      exchange -> exchange.getSymbols().stream()
          .filter(symbol -> StringUtils.equals(symbol.getStatus(), VALID_SYMBOL_STATUS))
          .map(BinanceSpotSymbol::getSymbol).toList());

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private RateLimitManager rateLimitManager;

  @Autowired
  private BinanceSpotClient binanceSpotClient;

  @Override
  public void afterPropertiesSet() throws Exception {
    refreshServerTimeDelta();
    serverTimeRefresher.scheduleAtFixedRate(new ExceptionSafeRunnable(this::refreshServerTimeDelta), 5, 3600, TimeUnit.SECONDS);
  }

  @Override
  public BinanceSpotExchange queryExchange() {
    BinanceSpotExchange exchange = cacheLoads.get(exchangeCache, StringUtils.EMPTY);
    exchange.setServerTime(queryServerTime());
    return exchange;
  }

  @Override
  public long queryServerTime() {
    return System.currentTimeMillis() - serverTimeDelta.get();
  }

  @Override
  public List<String> querySymbols() {
    return tradingSymbols.symbols(queryExchange());
  }

  @Override
  public Set<String> querySymbolSet() {
    return tradingSymbols.symbolSet(queryExchange());
  }


  private void refreshServerTimeDelta() {
    Call<BinanceServerTime> serverTimeCall = binanceSpotClient.getServerTime();
    BinanceServerTime serverTime = ClientUtil.getResponseBody(serverTimeCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_SPOT_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    if (serverTime.getServerTime() != null) {
      long deltaMills = System.currentTimeMillis() - serverTime.getServerTime();
      serverTimeDelta.set(deltaMills);
    }
  }

  private LoadingCache<String, BinanceSpotExchange> buildExchangeCache() {
    return Caffeine.newBuilder()
        .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<>() {
          @Override
          public @Nullable BinanceSpotExchange load(String s) throws Exception {
            Call<BinanceSpotExchange> exchangeCall = binanceSpotClient.getExchange();
            return ClientUtil.getResponseBody(exchangeCall,
                () -> rateLimitManager.stopAcquire(Constants.BINANCE_SPOT_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
          }
        });
  }
}
