package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotServerTime;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.util.List;
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
public class BinanceSpotController extends GenericController {

  private static final int DEFAULT_LIMIT = 100;

  @Autowired
  @Qualifier("binanceSpotKlineService")
  private KlineService klineService;

  @Autowired
  private ExchangeService<BinanceSpotExchange> exchangeService;

  @GetMapping("exchangeInfo")
  public BinanceSpotExchange queryExchange() {
    return exchangeService.queryExchange();
  }

  @GetMapping("time")
  public BinanceSpotServerTime queryTime() {
    return new BinanceSpotServerTime(exchangeService.queryServerTime());
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

