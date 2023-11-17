package com.zx.quant.klineproxy.service.impl;

import com.google.common.collect.Lists;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.ws.client.BinanceSpotWebSocketClient;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import retrofit2.Call;

/**
 * future kline service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceSpotKlineService")
public class BinanceSpotKlineServiceImpl extends AbstractKlineService<BinanceSpotWebSocketClient>
    implements KlineService, InitializingBean {

  @Autowired
  private ExchangeService<BinanceSpotExchange> exchangeService;

  @Autowired
  private BinanceSpotClient binanceSpotClient;

  @Override
  public List<Kline> queryKlines0(String symbol, String interval, Long startTime, Long endTime, Integer limit) {
    Call<List<Object[]>> klinesCall = binanceSpotClient.getKlines(symbol, interval, startTime, endTime, getLimit(limit));
    List<Object[]> responseBody = ClientUtil.getResponseBody(klinesCall);
    return Objects.requireNonNull(responseBody).stream()
        .map(this::serverKlineToKline)
        .collect(Collectors.toList());
  }

  @Override
  protected List<String> getSymbols() {
    return exchangeService.querySymbols().stream()
        .filter(Objects::isNull)
        .toList();
  }

  @Override
  protected List<IntervalEnum> getSubscribeIntervals() {
    return Lists.newArrayList(
        IntervalEnum.ONE_MINUTE,
        IntervalEnum.ONE_HOUR
    );
  }
}
