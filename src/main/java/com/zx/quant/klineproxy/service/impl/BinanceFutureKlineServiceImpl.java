package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.ws.client.BinanceFutureWebSocketClient;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * future kline service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceFutureKlineService")
public class BinanceFutureKlineServiceImpl extends AbstractKlineService<BinanceFutureWebSocketClient>
    implements KlineService, InitializingBean {

  @Autowired
  private ExchangeService<BinanceFutureExchange> exchangeService;

  @Autowired
  private BinanceFutureClient binanceFutureClient;

  @Override
  public List<Kline> queryKlines(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    Call<List<Object[]>> klinesCall = binanceFutureClient.getKlines(symbol, interval, startTime, endTime, getLimit(limit));
    List<Object[]> responseBody = ClientUtil.getResponseBody(klinesCall);
    return Objects.requireNonNull(responseBody).stream()
        .map(this::serverKlineToKline).collect(Collectors.toList());
  }

  @Override
  protected List<String> getSymbols() {
//    return exchangeService.querySymbols();
    return Collections.emptyList();
  }


  @Override
  protected Consumer<String> getMessageHandler() {
    return message -> log.info("future message: {}", message);
  }

  @Override
  protected String buildSymbolUpdateTopic(String symbol) {
    return StringUtils.lowerCase(symbol) + "@miniTicker";
  }

  @Override
  public void afterPropertiesSet() throws Exception {

  }
}
