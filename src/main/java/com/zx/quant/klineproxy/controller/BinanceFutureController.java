package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureServerTime;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.util.List;
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
public class BinanceFutureController extends GenericController {

  private static final int DEFAULT_LIMIT = 500;

  @Autowired
  @Qualifier("binanceFutureKlineService")
  private KlineService klineService;

  @Autowired
  private FutureExchangeService<BinanceFutureExchange> exchangeService;

  @GetMapping("exchangeInfo")
  public BinanceFutureExchange queryExchange() {
    return exchangeService.queryExchange();
  }

  @GetMapping("time")
  public BinanceFutureServerTime queryTime() {
    return new BinanceFutureServerTime(exchangeService.queryServerTime());
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
      validateSymbols(List.of(symbol), exchangeService.querySymbols());
      return exchangeService.queryPremiumIndex(symbol);
    } else {
      return exchangeService.queryPremiumIndices();
    }
  }

  @GetMapping("ticker/24hr")
  public Object queryTicker24Hr(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    List<String> realSymbols = StringUtils.isNotBlank(symbol) ? List.of(symbol) : List.of();
    validateSymbols(realSymbols, exchangeService.querySymbols());
    List<Ticker24Hr> ticker24Hrs = klineService.queryTicker24hrs(realSymbols);
    return ConvertUtil.convertToDisplayTicker24hr(ticker24Hrs, shouldReturnArray(symbol));
  }

  @GetMapping("ticker/price")
  public Object queryTicker(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    List<String> realSymbols = StringUtils.isNotBlank(symbol) ? List.of(symbol) : List.of();
    validateSymbols(realSymbols, exchangeService.querySymbols());
    List<Ticker<?>> tickers = klineService.queryTickers(realSymbols);
    return ConvertUtil.convertToDisplayTicker(tickers, shouldReturnArray(symbol));
  }

  @GetMapping("klines")
  public Object[][] queryKlines(
      @RequestParam(value = "symbol") String symbol,
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    validateSymbols(List.of(symbol), exchangeService.querySymbols());
    int realLimit = limit != null ? limit : DEFAULT_LIMIT;
    Kline[] klines = klineService.queryKlineArray(symbol, interval, startTime, endTime, realLimit);
    Object[][] displayKlines = new Object[klines.length][];
    for(int i = 0; i < klines.length; i++) {
      Kline kline = klines[i];
      Object[] displayKline = ConvertUtil.convertToDisplayKline(kline);
      displayKlines[i] = displayKline;
    }
    return displayKlines;
  }
}
