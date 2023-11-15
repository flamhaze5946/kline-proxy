package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * binance future controller
 * @author flamhaze5946
 */
@RestController
@RequestMapping("fapi/v1")
public class BinanceFutureController {

  @Autowired
  @Qualifier("binanceFutureKlineService")
  private KlineService klineService;

  @Autowired
  private ExchangeService<BinanceFutureExchange> exchangeService;

  @GetMapping("exchangeInfo")
  public BinanceFutureExchange queryExchange() {
    return exchangeService.queryExchange();
  }

  @GetMapping("klines")
  public List<Object[]> queryKlines(
      @RequestParam(value = "symbol") String symbol,
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    List<Kline> klines = klineService.queryKlines(symbol, interval, startTime, endTime, limit);
    return klines.stream().map(this::convertToDisplayKline).collect(Collectors.toList());
  }

  private Object[] convertToDisplayKline(Kline kline) {
    if (kline == null) {
      return new Object[0];
    }

    return new Object[] {
        kline.getOpenTime(),
        kline.getOpenPrice().toPlainString(),
        kline.getHighPrice().toPlainString(),
        kline.getLowPrice().toPlainString(),
        kline.getClosePrice().toPlainString(),
        kline.getVolume().toPlainString(),
        kline.getCloseTime(),
        kline.getQuoteVolume().toPlainString(),
        kline.getTradeNum(),
        kline.getActiveBuyVolume(),
        kline.getActiveBuyQuoteVolume(),
        kline.getIgnore()
    };
  }
}

