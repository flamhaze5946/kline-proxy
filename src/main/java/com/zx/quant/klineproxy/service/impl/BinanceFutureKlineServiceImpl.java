package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.ws.client.BinanceFutureWebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceFutureKlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * future kline service impl
 * @author flamhaze5946
 */
@Slf4j
@EnableConfigurationProperties(BinanceFutureKlineSyncConfigProperties.class)
@Service("binanceFutureKlineService")
public class BinanceFutureKlineServiceImpl extends AbstractKlineService<BinanceFutureWebSocketClient>
    implements KlineService, InitializingBean {

  private static final String SERVICE_TYPE = "binanceFuture";

  private static final int MAKE_UP_KLINE_COUNT = 499;

  private static final int MAKE_UP_KLINE_WEIGHT = 2;

  private static final int TICKER_24HR_WEIGHT = 40;

  @Autowired
  private BinanceFutureKlineSyncConfigProperties klineSyncConfigProperties;

  @Autowired
  private ExchangeService<BinanceFutureExchange> exchangeService;

  @Autowired
  private BinanceFutureClient binanceFutureClient;


  @Override
  public List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    Call<List<Object[]>> klinesCall = binanceFutureClient.getKlines(symbol, interval, startTime, endTime, getLimit(limit));
    List<Object[]> responseBody = ClientUtil.getResponseBody(klinesCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
    return Objects.requireNonNull(responseBody).stream()
        .map(this::serverKlineToKline)
        .collect(Collectors.toList());
  }

  @Override
  protected List<Ticker24Hr> queryTicker24Hrs0() {
    Call<List<Ticker24Hr>> ticker24hrCall = binanceFutureClient.getTicker24hr();
    return ClientUtil.getResponseBody(ticker24hrCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1000 * 30));
  }

  @Override
  protected long getServerTime() {
    return exchangeService.queryServerTime();
  }

  @Override
  protected String getRateLimiterName() {
    return Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME;
  }

  @Override
  protected List<String> getSymbols() {
    return exchangeService.querySymbols();
  }

  @Override
  protected KlineSyncConfigProperties getSyncConfig() {
    return klineSyncConfigProperties;
  }

  @Override
  protected int getMakeUpKlinesLimit() {
    return MAKE_UP_KLINE_COUNT;
  }

  @Override
  protected int getMakeUpKlinesWeight() {
    return MAKE_UP_KLINE_WEIGHT;
  }

  @Override
  protected int getTicker24HrsWeight() {
    return TICKER_24HR_WEIGHT;
  }

  @Override
  protected String getServiceType() {
    return SERVICE_TYPE;
  }
}
