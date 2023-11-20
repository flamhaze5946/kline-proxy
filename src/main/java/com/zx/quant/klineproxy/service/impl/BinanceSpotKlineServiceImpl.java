package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.ws.client.BinanceSpotWebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties;
import com.zx.quant.klineproxy.model.config.KlineSyncConfigProperties.BinanceSpotKlineSyncConfigProperties;
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
@EnableConfigurationProperties(BinanceSpotKlineSyncConfigProperties.class)
@Service("binanceSpotKlineService")
public class BinanceSpotKlineServiceImpl extends AbstractKlineService<BinanceSpotWebSocketClient>
    implements KlineService, InitializingBean {

  private static final int MAKE_UP_KLINE_WEIGHT = 2;

  private static final int MAKE_UP_KLINE_COUNT = 1000;

  @Autowired
  private BinanceSpotKlineSyncConfigProperties klineSyncConfigProperties;

  @Autowired
  private ExchangeService<BinanceSpotExchange> exchangeService;

  @Autowired
  private BinanceSpotClient binanceSpotClient;

  @Override
  public List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    Call<List<Object[]>> klinesCall = binanceSpotClient.getKlines(symbol, interval, startTime, endTime, getLimit(limit));
    List<Object[]> responseBody = ClientUtil.getResponseBody(klinesCall,
        () -> rateLimitManager.stopAcquire(Constants.BINANCE_SPOT, 1000 * 30));
    return Objects.requireNonNull(responseBody).stream()
        .map(this::serverKlineToKline)
        .collect(Collectors.toList());
  }

  @Override
  protected long getServerTime() {
    return exchangeService.queryServerTime();
  }

  @Override
  protected String getRateLimiterName() {
    return Constants.BINANCE_FUTURE;
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
}
