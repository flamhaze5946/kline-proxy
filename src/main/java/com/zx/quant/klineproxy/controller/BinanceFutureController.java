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
import java.util.ArrayList;
import java.util.List;
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
public class BinanceFutureController extends GenericController {

  private static final int DEFAULT_LIMIT = 100;

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
      return exchangeService.queryPremiumIndex(symbol);
    } else {
      return exchangeService.queryPremiumIndices();
    }
  }

  @GetMapping("ticker/24hr")
  public Object queryTicker24Hr(
      @RequestParam(value = "symbol", required = false) String symbol,
      @RequestParam(value = "symbols", required = false) List<String> symbols
  ) {
    List<String> realSymbols = getRealSymbols(symbol, symbols);
    List<Ticker24Hr> ticker24Hrs = klineService.queryTicker24hrs(realSymbols);
    return ConvertUtil.convertToDisplayTicker24hr(ticker24Hrs);
  }

  @GetMapping("ticker/price")
  public Object queryTicker(
      @RequestParam(value = "symbol", required = false) String symbol,
      @RequestParam(value = "symbols", required = false) List<String> symbols
  ) {
    List<String> realSymbols = getRealSymbols(symbol, symbols);
    List<Ticker<?>> tickers = klineService.queryTickers(realSymbols);
    return ConvertUtil.convertToDisplayTicker(tickers);
  }

  @GetMapping("klines")
  public Object[][] queryKlines(
      @RequestParam(value = "symbol") String symbol,
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    int realLimit = limit != null ? limit : DEFAULT_LIMIT;
    Kline<?>[] klines = klineService.queryKlineArray(symbol, interval, startTime, endTime, realLimit);
    Object[][] displayKlines = new Object[klines.length][];
    for(int i = 0; i < klines.length; i++) {
      Kline<?> kline = klines[i];
      Object[] displayKline = ConvertUtil.convertToDisplayKline(kline);
      displayKlines[i] = displayKline;
    }
    return displayKlines;
  }
}

