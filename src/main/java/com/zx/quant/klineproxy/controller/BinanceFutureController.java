package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.client.model.BinanceFutureServerTime;
import com.zx.quant.klineproxy.model.BulkFundingRateRequest;
import com.zx.quant.klineproxy.model.BulkFundingRateResponse;
import com.zx.quant.klineproxy.model.BulkKlinesRequest;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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

  @Autowired
  private Serializer serializer;

  private final AtomicReference<RenderedJsonResponse<List<Ticker<?>>>> allMarketTickerResponse =
      new AtomicReference<>();

  private final AtomicReference<RenderedJsonResponse<List<Ticker24Hr>>> allMarketTicker24HrResponse =
      new AtomicReference<>();

  @GetMapping("exchangeInfo")
  public BinanceFutureExchange queryExchange() {
    return exchangeService.queryExchange();
  }

  @GetMapping("time")
  public BinanceFutureServerTime queryTime() {
    return new BinanceFutureServerTime(exchangeService.queryServerTime());
  }

  @GetMapping("fundingRate")
  public Object queryFundingRate(
      @RequestParam(value = "symbol", required = false) String symbol,
      @RequestParam(value = "startTime", required = false) Long startTime,
      @RequestParam(value = "endTime", required = false) Long endTime,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    if (StringUtils.isNotBlank(symbol)) {
      validateSymbols(List.of(symbol), allSymbols(exchangeService.queryExchange()));
    }
    List<FutureFundingRate> fundingRates = exchangeService.queryFundingRates(symbol, startTime, endTime, limit);
    return ConvertUtil.convertToDisplayFundingRates(fundingRates);
  }

  @GetMapping("fundingRate/bulk")
  public BulkFundingRateResponse queryBulkFundingRate(
      @RequestParam(value = "symbols", required = false) String symbols,
      @RequestParam(value = "since_ms", required = false) Long sinceMs,
      @RequestParam(value = "until_ms", required = false) Long untilMs,
      @RequestParam(value = "limit", required = false) Integer limit
  ) {
    return exchangeService.queryBulkFundingRates(parseCsv(symbols), sinceMs, untilMs, limit);
  }

  /**
   * bulk funding rate via post body
   * @author flamhaze5946
   */
  @PostMapping("fundingRate/bulk")
  public BulkFundingRateResponse queryBulkFundingRatePost(
      @RequestBody(required = false) BulkFundingRateRequest body
  ) {
    BulkFundingRateRequest req = body != null ? body
        : new BulkFundingRateRequest(null, null, null, null);
    return exchangeService.queryBulkFundingRates(
        normalizeSymbolList(req.symbols()),
        req.sinceMs(),
        req.untilMs(),
        req.limit());
  }

  @GetMapping("premiumIndex")
  public Object queryPremiumIndex(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    if (StringUtils.isNotBlank(symbol)) {
      validateSymbols(List.of(symbol), allSymbols(exchangeService.queryExchange()));
      return ConvertUtil.convertToDisplayPremiumIndex(exchangeService.queryPremiumIndex(symbol));
    } else {
      return ConvertUtil.convertToDisplayPremiumIndices(exchangeService.queryPremiumIndices());
    }
  }

  @GetMapping("ticker/24hr")
  public Object queryTicker24Hr(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    List<String> realSymbols = StringUtils.isNotBlank(symbol) ? List.of(symbol) : List.of();
    validateSymbols(realSymbols, allSymbols(exchangeService.queryExchange()));
    List<Ticker24Hr> ticker24Hrs = klineService.queryTicker24hrs(realSymbols);
    if (realSymbols.isEmpty()) {
      return renderAllMarketTicker24HrResponse(ticker24Hrs);
    }
    return ConvertUtil.convertToDisplayTicker24hr(ticker24Hrs, shouldReturnArray(symbol));
  }

  @GetMapping("ticker/price")
  public Object queryTicker(
      @RequestParam(value = "symbol", required = false) String symbol
  ) {
    List<String> realSymbols = StringUtils.isNotBlank(symbol) ? List.of(symbol) : List.of();
    validateSymbols(realSymbols, allSymbols(exchangeService.queryExchange()));
    List<Ticker<?>> tickers = klineService.queryTickers(realSymbols);
    if (realSymbols.isEmpty()) {
      return renderAllMarketTickerResponse(tickers);
    }
    return ConvertUtil.convertToDisplayTicker(tickers, shouldReturnArray(symbol));
  }

  @GetMapping("klines/bulk")
  public BulkKlinesResponse queryBulkKlines(
      @RequestParam(value = "interval") String interval,
      @RequestParam(value = "limit", required = false) Integer limit,
      @RequestParam(value = "closed_only", required = false, defaultValue = "true") Boolean closedOnly,
      @RequestParam(value = "symbols", required = false) String symbols
  ) {
    return klineService.queryBulkKlines(interval, limit, Boolean.TRUE.equals(closedOnly), parseCsv(symbols));
  }

  /**
   * bulk klines via post body
   * @author flamhaze5946
   */
  @PostMapping("klines/bulk")
  public BulkKlinesResponse queryBulkKlinesPost(
      @RequestBody(required = false) BulkKlinesRequest body
  ) {
    if (body == null || StringUtils.isBlank(body.interval())) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1102, "interval is required");
    }
    boolean closedOnly = body.closedOnly() == null || Boolean.TRUE.equals(body.closedOnly());
    return klineService.queryBulkKlines(
        body.interval(),
        body.limit(),
        closedOnly,
        normalizeSymbolList(body.symbols()));
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

  private List<String> parseCsv(String symbols) {
    if (StringUtils.isBlank(symbols)) {
      return List.of();
    }
    return Arrays.stream(symbols.split(","))
        .map(StringUtils::trim)
        .filter(StringUtils::isNotBlank)
        .distinct()
        .sorted()
        .toList();
  }

  private List<String> normalizeSymbolList(List<String> symbols) {
    if (symbols == null || symbols.isEmpty()) {
      return List.of();
    }
    LinkedHashSet<String> deduped = new LinkedHashSet<>();
    for (String s : symbols) {
      if (s == null) {
        continue;
      }
      String trimmed = StringUtils.trim(s);
      if (StringUtils.isNotBlank(trimmed)) {
        deduped.add(trimmed);
      }
    }
    if (deduped.isEmpty()) {
      return List.of();
    }
    List<String> out = new ArrayList<>(deduped);
    Collections.sort(out);
    return List.copyOf(out);
  }

  private List<String> allSymbols(BinanceFutureExchange exchange) {
    if (exchange == null || exchange.getSymbols() == null || exchange.getSymbols().isEmpty()) {
      return exchangeService.querySymbols();
    }
    return exchange.getSymbols().stream()
        .map(BinanceFutureSymbol::getSymbol)
        .toList();
  }

  private Object renderAllMarketTickerResponse(List<Ticker<?>> tickers) {
    RenderedJsonResponse<List<Ticker<?>>> cachedResponse = allMarketTickerResponse.get();
    if (cachedResponse != null && cachedResponse.source() == tickers) {
      return cachedResponse.response();
    }
    String payload = serializer.toJsonString(ConvertUtil.convertToDisplayTicker(tickers, true));
    ResponseEntity<String> response = ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(payload);
    allMarketTickerResponse.set(new RenderedJsonResponse<>(tickers, response));
    return response;
  }

  private Object renderAllMarketTicker24HrResponse(List<Ticker24Hr> ticker24Hrs) {
    RenderedJsonResponse<List<Ticker24Hr>> cachedResponse = allMarketTicker24HrResponse.get();
    if (cachedResponse != null && cachedResponse.source() == ticker24Hrs) {
      return cachedResponse.response();
    }
    String payload = serializer.toJsonString(ConvertUtil.convertToDisplayTicker24hr(ticker24Hrs, true));
    ResponseEntity<String> response = ResponseEntity.ok()
        .contentType(MediaType.APPLICATION_JSON)
        .body(payload);
    allMarketTicker24HrResponse.set(new RenderedJsonResponse<>(ticker24Hrs, response));
    return response;
  }

  private record RenderedJsonResponse<T>(T source, ResponseEntity<String> response) {
  }
}
