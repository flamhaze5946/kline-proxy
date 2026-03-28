package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotSymbol;
import com.zx.quant.klineproxy.client.model.BinanceSpotServerTime;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ClientUtil;
import com.zx.quant.klineproxy.util.ConvertUtil;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import retrofit2.Call;

/**
 * binance future controller
 * @author flamhaze5946
 */
@RestController
@RequestMapping("api/v3")
public class BinanceSpotController extends GenericController {

  private static final int DEFAULT_LIMIT = 500;

  @Autowired
  @Qualifier("binanceSpotKlineService")
  private KlineService klineService;

  @Autowired
  private ExchangeService<BinanceSpotExchange> exchangeService;

  @Autowired
  private com.zx.quant.klineproxy.client.BinanceSpotClient binanceSpotClient;

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
      @RequestParam(value = "symbols", required = false) String symbols,
      @RequestParam(value = "type", required = false) String type,
      @RequestParam(value = "symbolStatus", required = false) String symbolStatus
  ) {
    boolean explicitSymbolRequest = StringUtils.isNotBlank(symbol) || StringUtils.isNotBlank(symbols);
    validateTickerType(type);
    validateSymbolStatus(symbolStatus);
    List<String> realSymbols = getRealSymbols(symbol, symbols);
    Map<String, BinanceSpotSymbol> symbolMap = allSymbolMap(exchangeService.queryExchange());
    validateSymbols(realSymbols, symbolMap.keySet());
    List<String> requestSymbols = filterSymbolsByStatus(realSymbols, symbolStatus, symbolMap, StringUtils.isNotBlank(symbol));
    if (explicitSymbolRequest && CollectionUtils.isEmpty(requestSymbols)) {
      return ConvertUtil.convertToDisplayTicker24hr(List.of(), shouldReturnArray(symbol), StringUtils.equals(type, "MINI"));
    }
    List<Ticker24Hr> ticker24Hrs = klineService.queryTicker24hrs(requestSymbols);
    if (!explicitSymbolRequest && StringUtils.isNotBlank(symbolStatus)) {
      ticker24Hrs = ticker24Hrs.stream()
          .filter(ticker24Hr -> statusMatches(ticker24Hr.getSymbol(), symbolStatus, symbolMap))
          .collect(Collectors.toList());
    }
    return ConvertUtil.convertToDisplayTicker24hr(ticker24Hrs, shouldReturnArray(symbol), StringUtils.equals(type, "MINI"));
  }

  @GetMapping("ticker/price")
  public Object queryTicker(
      @RequestParam(value = "symbol", required = false) String symbol,
      @RequestParam(value = "symbols", required = false) String symbols
  ) {
    List<String> realSymbols = getRealSymbols(symbol, symbols);
    validateSymbols(realSymbols, allSymbols(exchangeService.queryExchange()));
    List<Ticker<?>> tickers = klineService.queryTickers(realSymbols);
    return ConvertUtil.convertToDisplayTicker(tickers, shouldReturnArray(symbol), false);
  }

  @GetMapping("klines")
  public Object[][] queryKlines(
      @RequestParam(value = "symbol") String symbol,
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit,
      @RequestParam(value = "timeZone", required = false) String timeZone
  ) {
    validateSymbols(List.of(symbol), allSymbols(exchangeService.queryExchange()));
    int realLimit = limit != null ? limit : DEFAULT_LIMIT;
    if (StringUtils.isNotBlank(timeZone) || StringUtils.equals(interval, IntervalEnum.ONE_MONTH.code())) {
      Call<List<Object[]>> klinesCall = binanceSpotClient.getKlines(symbol, interval, startTime, endTime, realLimit, timeZone);
      List<Object[]> responseBody = ClientUtil.getResponseBody(klinesCall);
      return responseBody.toArray(Object[][]::new);
    }
    Kline[] klines = klineService.queryKlineArray(symbol, interval, startTime, endTime, realLimit);
    Object[][] displayKlines = new Object[klines.length][];
    for(int i = 0; i < klines.length; i++) {
      Kline kline = klines[i];
      Object[] displayKline = ConvertUtil.convertToDisplayKline(kline);
      displayKlines[i] = displayKline;
    }
    return displayKlines;
  }

  private List<String> allSymbols(BinanceSpotExchange exchange) {
    if (exchange == null || CollectionUtils.isEmpty(exchange.getSymbols())) {
      return exchangeService.querySymbols();
    }
    return exchange.getSymbols().stream()
        .map(BinanceSpotSymbol::getSymbol)
        .collect(Collectors.toList());
  }

  private Map<String, BinanceSpotSymbol> allSymbolMap(BinanceSpotExchange exchange) {
    if (exchange == null || CollectionUtils.isEmpty(exchange.getSymbols())) {
      return exchangeService.querySymbols().stream()
          .collect(Collectors.toMap(Function.identity(), symbol -> {
            BinanceSpotSymbol spotSymbol = new BinanceSpotSymbol();
            spotSymbol.setSymbol(symbol);
            return spotSymbol;
          }, (o, n) -> o));
    }
    return exchange.getSymbols().stream()
        .collect(Collectors.toMap(BinanceSpotSymbol::getSymbol, Function.identity()));
  }

  private List<String> filterSymbolsByStatus(List<String> symbols, String symbolStatus,
      Map<String, BinanceSpotSymbol> symbolMap, boolean singleSymbolRequest) {
    if (CollectionUtils.isEmpty(symbols) || StringUtils.isBlank(symbolStatus)) {
      return symbols;
    }
    if (singleSymbolRequest) {
      String requestSymbol = symbols.get(0);
      if (!statusMatches(requestSymbol, symbolStatus, symbolMap)) {
        throw new com.zx.quant.klineproxy.model.exceptions.ApiException(
            org.springframework.http.HttpStatus.BAD_REQUEST,
            -1220,
            "The symbol's status does not match the requested symbolStatus");
      }
      return symbols;
    }
    return symbols.stream()
        .filter(requestSymbol -> statusMatches(requestSymbol, symbolStatus, symbolMap))
        .collect(Collectors.toList());
  }

  private boolean statusMatches(String symbol, String symbolStatus, Map<String, BinanceSpotSymbol> symbolMap) {
    if (StringUtils.isBlank(symbolStatus)) {
      return true;
    }
    BinanceSpotSymbol symbolInfo = symbolMap.get(symbol);
    return symbolInfo != null && StringUtils.equals(symbolInfo.getStatus(), symbolStatus);
  }
}
