package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
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
  private FutureExchangeService<BinanceFutureExchange> exchangeService;

  @GetMapping("exchangeInfo")
  public BinanceFutureExchange queryExchange() {
    return exchangeService.queryExchange();
  }

  @GetMapping("fundingRate")
  public List<FutureFundingRate> queryFundingRate() {
    return exchangeService.queryFundingRates();
  }

  @GetMapping("premiumIndex")
  public Object queryPremiumIndex(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    if (StringUtils.isNotBlank(symbol)) {
      return exchangeService.queryPremiumIndex(symbol);
    } else {
      return exchangeService.queryPremiumIndices();
    }
  }

  @GetMapping("ticker/price")
  public Object queryTicker(
      @RequestParam(value = "symbol", required = false) String symbol,
      @RequestParam(value = "symbols", required = false) List<String> symbols
  ) {
    List<String> realSymbols = new ArrayList<>();
    if (CollectionUtils.isNotEmpty(symbols)) {
      realSymbols.addAll(symbols);
    }
    if (symbol != null) {
      realSymbols.add(symbol);
    }

    realSymbols = realSymbols.stream()
        .distinct()
        .toList();
    List<Ticker<?>> tickers = klineService.queryTickers(realSymbols);
    return ConvertUtil.convertToDisplayTicker(tickers);
  }

  @GetMapping("klines")
  public List<Object[]> queryKlines(
      @RequestParam(value = "symbol") String symbol,
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    List<Kline<?>> klines = klineService.queryKlines(symbol, interval, startTime, endTime, limit);
    return klines.stream().map(ConvertUtil::convertToDisplayKline).collect(Collectors.toList());
  }
}

