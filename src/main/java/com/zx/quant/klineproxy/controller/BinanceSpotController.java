package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
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
@RequestMapping("api/v3")
public class BinanceSpotController {

  @Autowired
  @Qualifier("binanceSpotKlineService")
  private KlineService klineService;

  @Autowired
  private ExchangeService<BinanceSpotExchange> exchangeService;

  @GetMapping("exchangeInfo")
  public BinanceSpotExchange queryExchange() {
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
    return klines.stream().map(ConvertUtil::convertToDisplayKline).collect(Collectors.toList());
  }
}

