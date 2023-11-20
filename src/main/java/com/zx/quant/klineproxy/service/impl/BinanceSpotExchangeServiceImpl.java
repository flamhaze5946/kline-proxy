package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotSymbol;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * binance spot exchange service impl
 * @author flamhaze5946
 */
@Service("binanceSpotExchangeService")
public class BinanceSpotExchangeServiceImpl implements ExchangeService<BinanceSpotExchange> {

  private static final String VALID_SYMBOL_STATUS = "TRADING";

  private final LoadingCache<String, BinanceSpotExchange> exchangeCache = buildExchangeCache();

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private RateLimitManager rateLimitManager;

  @Autowired
  private BinanceSpotClient binanceSpotClient;

  @Override
  public BinanceSpotExchange queryExchange() {
    BinanceSpotExchange exchange = exchangeCache.get(StringUtils.EMPTY);
    long serverTime = System.currentTimeMillis() - serverTimeDelta.get();
    exchange.setServerTime(serverTime);
    return exchange;
  }

  @Override
  public long queryServerTime() {
    Long serverTime = queryExchange().getServerTime();
    if (serverTime != null) {
      return serverTime;
    }
    return System.currentTimeMillis();
  }

  @Override
  public List<String> querySymbols() {
    return queryExchange().getSymbols().stream()
        .filter(symbol -> StringUtils.equals(symbol.getStatus(), VALID_SYMBOL_STATUS))
        .map(BinanceSpotSymbol::getSymbol)
        .collect(Collectors.toList());
  }

  private LoadingCache<String, BinanceSpotExchange> buildExchangeCache() {
    return Caffeine.newBuilder()
        .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<>() {
          @Override
          public @Nullable BinanceSpotExchange load(String s) throws Exception {
            Call<BinanceSpotExchange> exchangeCall = binanceSpotClient.getExchange();
            BinanceSpotExchange exchange = ClientUtil.getResponseBody(exchangeCall,
                () -> rateLimitManager.stopAcquire(Constants.BINANCE_SPOT, 1000 * 30));
            if (exchange.getServerTime() != null) {
              long deltaMills = System.currentTimeMillis() - exchange.getServerTime();
              serverTimeDelta.set(deltaMills);
            }
            return exchange;
          }
        });
  }
}
