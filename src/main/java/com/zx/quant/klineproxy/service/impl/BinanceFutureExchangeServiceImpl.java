package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.FuturePremiumIndex;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * binance future exchange service impl
 * @author flamhaze5946
 */
@Service("binanceFutureExchangeService")
public class BinanceFutureExchangeServiceImpl implements FutureExchangeService<BinanceFutureExchange> {

  private static final int MAX_FUNDING_RATE_LIMIT = 1000;

  private static final String VALID_SYMBOL_STATUS = "TRADING";

  private final LoadingCache<String, BinanceFutureExchange> exchangeCache = buildExchangeCache();

  private final LoadingCache<String, List<FutureFundingRate>> fundingRatesCache = buildFundingRatesCache();

  private final LoadingCache<String, Map<String, FuturePremiumIndex>> premiumIndicesCache = buildPremiumIndicesCache();

  private final AtomicLong serverTimeDelta = new AtomicLong(0);

  @Autowired
  private BinanceFutureClient binanceFutureClient;

  @Autowired
  private RateLimitManager rateLimitManager;

  @Override
  public BinanceFutureExchange queryExchange() {
    BinanceFutureExchange exchange = exchangeCache.get(StringUtils.EMPTY);
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
  public List<FutureFundingRate> queryFundingRates() {
    return fundingRatesCache.get(StringUtils.EMPTY);
  }

  @Override
  public List<FuturePremiumIndex> queryPremiumIndices() {
    return Lists.newArrayList(premiumIndicesCache.get(StringUtils.EMPTY).values());
  }

  @Override
  public FuturePremiumIndex queryPremiumIndex(String symbol) {
    return premiumIndicesCache.get(StringUtils.EMPTY).get(symbol);
  }

  @Override
  public List<String> querySymbols() {
    return queryExchange().getSymbols().stream()
        .filter(symbol -> StringUtils.equals(symbol.getStatus(), VALID_SYMBOL_STATUS))
        .map(BinanceFutureSymbol::getSymbol)
        .collect(Collectors.toList());
  }

  private LoadingCache<String, BinanceFutureExchange> buildExchangeCache() {
    return Caffeine.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(s -> {
          Call<BinanceFutureExchange> exchangeCall = binanceFutureClient.getExchange();
          BinanceFutureExchange exchange = ClientUtil.getResponseBody(exchangeCall,
              () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
          if (exchange.getServerTime() != null) {
            long deltaMills = System.currentTimeMillis() - exchange.getServerTime();
            serverTimeDelta.set(deltaMills);
          }
          return exchange;
        });
  }

  private LoadingCache<String, List<FutureFundingRate>> buildFundingRatesCache() {
    return Caffeine.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(Duration.of(10, ChronoUnit.MINUTES))
        .refreshAfterWrite(5, TimeUnit.MINUTES)
        .build(s -> {
          Call<List<FutureFundingRate>> ratesCall = binanceFutureClient.getFundingRates(MAX_FUNDING_RATE_LIMIT);
          List<FutureFundingRate> rates = ClientUtil.getResponseBody(ratesCall,
              () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
          Map<String, FutureFundingRate> symbolRateMap = rates.stream()
              .collect(Collectors.toMap(FutureFundingRate::getSymbol, Function.identity(), (o, n) -> {
                if (o.getFundingTime() == null) {
                  return n;
                }
                if (n.getFundingTime() == null) {
                  return o;
                }
                if (o.getFundingTime() > n.getFundingTime()) {
                  return o;
                }
                return n;
              }));
          return Collections.unmodifiableList(Lists.newArrayList(symbolRateMap.values()));
        });
  }

  private LoadingCache<String, Map<String, FuturePremiumIndex>> buildPremiumIndicesCache() {
    return Caffeine.newBuilder()
        .maximumSize(1)
        .expireAfterWrite(Duration.of(20, ChronoUnit.MINUTES))
        .refreshAfterWrite(10, TimeUnit.MINUTES)
        .build(s -> {
          Call<List<FuturePremiumIndex>> indicesCall = binanceFutureClient.getSymbolPremiumIndices();
          List<FuturePremiumIndex> indices = ClientUtil.getResponseBody(indicesCall,
              () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
          return indices.stream()
              .collect(Collectors.toMap(FuturePremiumIndex::getSymbol, Function.identity()));
        });
  }
}
