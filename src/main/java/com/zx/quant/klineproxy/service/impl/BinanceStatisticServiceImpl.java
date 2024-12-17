package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.BlockChainCenterClient;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.constant.Constants;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.statistic.YamaDateSymbolInfo;
import com.zx.quant.klineproxy.model.statistic.Yama01Index;
import com.zx.quant.klineproxy.model.statistic.Yama02Index;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.service.StatisticService;
import com.zx.quant.klineproxy.util.ClientUtil;
import com.zx.quant.klineproxy.util.Serializer;
import io.netty.util.internal.StringUtil;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * binance statistic service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceStatisticService")
public class BinanceStatisticServiceImpl implements StatisticService {

  private static final IntervalEnum ATR_INTERVAL = IntervalEnum.ONE_HOUR;

  private static final IntervalEnum ALT_COIN_INDEX_INTERVAL = IntervalEnum.ONE_DAY;

  private static final String YAMA_01_ALT_COIN_STANDARD_SYMBOL = "BTCUSDT";

  private final LoadingCache<String, Map<String, Float>> altCoinIndexCache = Caffeine.newBuilder()
      .maximumSize(4)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build(key -> getAltCoinIndex0());

  private final LoadingCache<String, Map<Long, Yama01Index>> yama01altCoinIndexCache = Caffeine.newBuilder()
      .maximumSize(4)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build(key -> getYama01AltCoinIndex0());

  private final LoadingCache<String, Map<Long, Yama02Index>> yama02altCoinIndexCache = Caffeine.newBuilder()
      .maximumSize(4)
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build(key -> getYama02AltCoinIndex0());

  @Autowired
  private Serializer serializer;

  @Autowired
  private BlockChainCenterClient blockChainCenterClient;

  @Qualifier("binanceSpotKlineService")
  @Autowired
  private KlineService binanceSpotKlineService;

  @Qualifier("binanceFutureKlineService")
  @Autowired
  private KlineService binanceFutureKlineService;

  @Value("${statistic.binance.atr.symbol:BTCUSDT}")
  private String atrSymbol;

  @Value("${statistic.binance.atr.period:48}")
  private int atrPeriod;

  @Value("${statistic.altCoinIndex.startDate:2021-01-01}")
  private String altCoinIndexStartDate;

  @Value("${statistic.yama01altCoinIndex.statisticDays:30}")
  private int yama01altCoinIndexStatisticDays;

  @Value("${statistic.yamaAltCoinIndex.quoteVolumeStatisticDays:7}")
  private int yamaAltCoinIndexQuoteVolumeStatisticDays;

  @Value("${statistic.yama02altCoinIndex.quoteVolumeMaxRank:20}")
  private int yama02altCoinIndexQuoteVolumeMaxRank;

  @Qualifier("binanceSpotExchangeService")
  @Autowired
  private ExchangeService<BinanceSpotExchange> binanceSpotExchangeService;

  @Qualifier("binanceFutureExchangeService")
  @Autowired
  private ExchangeService<BinanceFutureExchange> binanceFutureExchangeService;

  @Scheduled(fixedDelay = 3600000)
  @Override
  public void recordAtr() {
    List<BigDecimal> atrs = new ArrayList<>();
    Kline[] klineArray = binanceSpotKlineService.queryKlineArray(
        atrSymbol, ATR_INTERVAL.code(), null, System.currentTimeMillis(), atrPeriod);
    List<BigDecimalKline> klines = Arrays.stream(klineArray)
        .map(kline -> {
          String klineJson = serializer.toJsonString(kline);
          return serializer.fromJsonString(klineJson, BigDecimalKline.class);
        })
        .toList();
    if (CollectionUtils.isEmpty(klines) || klines.size() < 2) {
      return;
    }
    klines = klines.subList(0, klines.size() - 1);

    List<BigDecimal> highs = klines.stream()
        .map(BigDecimalKline::getHighPrice)
        .toList();
    List<BigDecimal> lows = klines.stream()
        .map(BigDecimalKline::getLowPrice)
        .toList();
    List<BigDecimal> opens = klines.stream()
        .map(BigDecimalKline::getOpenPrice)
        .toList();
    List<BigDecimal> closes = klines.stream()
        .map(BigDecimalKline::getClosePrice)
        .toList();

    List<BigDecimal> trs = new ArrayList<>();
    for(int i = 0; i < klines.size(); i++) {
      BigDecimal currentHigh = highs.get(i);
      BigDecimal currentLow = lows.get(i);
      BigDecimal previousClose;
      if (i == 0) {
        previousClose = BigDecimal.ZERO;
      } else {
        previousClose = closes.get(i - 1);
      }
      BigDecimal c1 = currentHigh.subtract(currentLow);
      BigDecimal c2 = currentHigh.subtract(previousClose);
      BigDecimal c3 = currentLow.subtract(previousClose);
      BigDecimal tr = c1.max(c2).max(c3);
      trs.add(tr);
    }

    List<BigDecimal> closeRollingMeans = calculateRollingMeans(closes, atrPeriod);
    List<BigDecimal> trRollingMeans = calculateRollingMeans(trs, atrPeriod);

    for(int i = 0; i < klines.size(); i++) {
      BigDecimal closeMean = closeRollingMeans.get(i);
      BigDecimal trMean = trRollingMeans.get(i);
      BigDecimal atr = trMean.divide(closeMean.add(Constants.EPS), Constants.SCALE, RoundingMode.DOWN);
      atrs.add(atr);
    }
    log.info("atr: {}", atrs.get(atrs.size() - 1));
  }

  @Override
  public Map<String, Float> getAltCoinIndex() {
    return altCoinIndexCache.get(StringUtil.EMPTY_STRING);
  }

  @Override
  public Map<Long, Yama01Index> getYama01AltCoinIndex() {
    return yama01altCoinIndexCache.get(StringUtil.EMPTY_STRING);
  }

  @Override
  public Map<Long, Yama02Index> getYama02AltCoinIndex() {
    return yama02altCoinIndexCache.get(StringUtil.EMPTY_STRING);
  }

  private Map<Long, Yama01Index> getYama01AltCoinIndex0() {
    List<String> symbols = getYamaIndexSymbols();
    LocalDateTime todayTime = LocalDate.now().atStartOfDay();
    LocalDateTime startTime = todayTime.minusDays(yama01altCoinIndexStatisticDays);
    LocalDateTime klineStartTime = startTime.minusDays(yama01altCoinIndexStatisticDays);
    Long todayDate = todayTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    Long startDate = startTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    Long klineStartDate = klineStartTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    NavigableMap<Long, Yama01Index> dateIndexMap = calculateYama01DateIndexMap(symbols, klineStartDate, todayDate);
    return dateIndexMap.tailMap(startDate);
  }

  private Map<Long, Yama02Index> getYama02AltCoinIndex0() {
    List<String> symbols = getYamaIndexSymbols();
    LocalDateTime todayTime = LocalDate.now().atStartOfDay();
    LocalDateTime startTime = todayTime.minusDays(yama01altCoinIndexStatisticDays);
    LocalDateTime klineStartTime = startTime.minusDays(yama01altCoinIndexStatisticDays);
    Long todayDate = todayTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    Long startDate = startTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    Long klineStartDate = klineStartTime.atZone(ZoneOffset.systemDefault()).toInstant().toEpochMilli();
    NavigableMap<Long, Yama02Index> dateIndexMap = calculateYama02DateIndexMap(symbols, klineStartDate, todayDate);
    return dateIndexMap.tailMap(startDate);
  }

  @SuppressWarnings("unchecked")
  private Map<String, Float> getAltCoinIndex0() {
    try{
      ResponseBody responseBody = ClientUtil.getResponseBody(
          blockChainCenterClient.getRawAltCoinSeasonIndex());
      String pageContent = responseBody.string();
      String mapRaw =
          pageContent.split("chartdata\\[30] = ")[1].split(";\\n\\t\\t\\t\\tchartdata2")[0];
      Map<String, Map<String, List<String>>> indexKeyValueMap = serializer.fromJsonString(mapRaw, LinkedHashMap.class);
      Map<String, List<String>> labelsMap = indexKeyValueMap.get("labels");
      Map<String, List<String>> valuesMap = indexKeyValueMap.get("values");
      List<String> allLabels = labelsMap.get("all");
      List<String> allValues = valuesMap.get("all");
      Map<String, Float> dateIndices = new LinkedHashMap<>();
      for(int i = 0; i < allLabels.size(); i++) {
        if (i >= allValues.size()) {
          break;
        }
        String date = allLabels.get(i);
        if (date.compareTo(altCoinIndexStartDate) < 0) {
          continue;
        }
        String dateAltCoinIndex = allValues.get(i);
        dateIndices.put(date, Float.parseFloat(dateAltCoinIndex) / 100);
      }
      return dateIndices;
    } catch (Exception e) {
      log.warn("query alt coin index failed.", e);
      return Collections.emptyMap();
    }
  }

  private List<String> getYamaIndexSymbols() {
    BinanceFutureExchange binanceFutureExchange = binanceFutureExchangeService.queryExchange();
    return binanceFutureExchange.getSymbols().stream()
        .filter(symbol -> StringUtils.equals(symbol.getStatus(), "TRADING"))
        .filter(symbol -> StringUtils.equals(symbol.getQuoteAsset(), "USDT"))
        .map(BinanceFutureSymbol::getSymbol)
        .toList();
  }

  private Map<Long, Map<String, YamaDateSymbolInfo>> calculateDateSymbolInfos(List<String> symbols, Long klineStartDate, Long klineEndDate) {
    Map<Long, Map<String, YamaDateSymbolInfo>> dateSymbolInfos = new HashMap<>();

    for (String symbol : symbols) {
      List<BigDecimalKline> klines = binanceFutureKlineService.queryKlineList(
              symbol, ALT_COIN_INDEX_INTERVAL.code(), klineStartDate, klineEndDate, Integer.MAX_VALUE).stream()
          .map(kline -> serializer.fromJsonString(serializer.toJsonString(kline), BigDecimalKline.class))
          .toList();
      if (CollectionUtils.isEmpty(klines)) {
        continue;
      }
      List<YamaDateSymbolInfo> yamaDateSymbolInfos = new ArrayList<>();
      for (int i = 0; i < klines.size(); i++) {
        BigDecimalKline currentKline = klines.get(i);
        BigDecimal priceChange = BigDecimal.ZERO;
        BigDecimal quoteVolumeSum = currentKline.getQuoteVolume();
        int priceCompareIndex = i - yama01altCoinIndexStatisticDays + 1;
        if (priceCompareIndex >= 0) {
          BigDecimalKline priceChangeCompareKline = klines.get(priceCompareIndex);
          priceChange = currentKline.getOpenPrice()
              .divide(priceChangeCompareKline.getOpenPrice(), 8, RoundingMode.DOWN)
              .subtract(BigDecimal.ONE);
        }
        for(int j = yamaAltCoinIndexQuoteVolumeStatisticDays - 1; j > 0 ; j--) {
          int accumulateKlineIndex = i - j;
          if (accumulateKlineIndex < 0) {
            continue;
          }
          BigDecimalKline previousKline = klines.get(accumulateKlineIndex);
          quoteVolumeSum = quoteVolumeSum.add(previousKline.getQuoteVolume());
        }
        YamaDateSymbolInfo yamaDateSymbolInfo = new YamaDateSymbolInfo();
        yamaDateSymbolInfo.setSymbol(symbol);
        yamaDateSymbolInfo.setPctChange(priceChange.floatValue());
        yamaDateSymbolInfo.setOpenTime(currentKline.getOpenTime());
        yamaDateSymbolInfo.setQuoteVolumeSum(quoteVolumeSum.floatValue());
        yamaDateSymbolInfos.add(yamaDateSymbolInfo);
        dateSymbolInfos
            .computeIfAbsent(currentKline.getOpenTime(), var -> new HashMap<>())
            .put(symbol, yamaDateSymbolInfo);
      }
      for(int i = 0; i < yamaDateSymbolInfos.size(); i++) {
        YamaDateSymbolInfo yamaDateSymbolInfo = yamaDateSymbolInfos.get(i);
        yamaDateSymbolInfo.setPctChangeSum(yamaDateSymbolInfo.getPctChange());
        for(int j = yamaAltCoinIndexQuoteVolumeStatisticDays - 1; j > 0 ; j--) {
          int accumulateIndex = i - j;
          if (accumulateIndex < 0) {
            continue;
          }
          YamaDateSymbolInfo accumulateInfo = yamaDateSymbolInfos.get(accumulateIndex);
          yamaDateSymbolInfo.setPctChangeSum(yamaDateSymbolInfo.getPctChangeSum() + accumulateInfo.getPctChange());
        }
      }
    }
    return dateSymbolInfos;
  }

  private NavigableMap<Long, Yama01Index> calculateYama01DateIndexMap(List<String> symbols, Long klineStartDate, Long klineEndDate) {
    NavigableMap<Long, Yama01Index> dateIndexMap = new TreeMap<>();
    Map<Long, Map<String, YamaDateSymbolInfo>> dateSymbolInfos = calculateDateSymbolInfos(symbols, klineStartDate, klineEndDate);
    dateSymbolInfos.forEach((openTime, symbolInfos) -> {
      Yama01Index index = new Yama01Index();
      index.setOpenTime(openTime);
      index.setTotalRank(symbolInfos.size());
      List<YamaDateSymbolInfo> sortedSymbolInfos = symbolInfos.values().stream()
          .sorted(Comparator.comparing(YamaDateSymbolInfo::getPctChange, Comparator.reverseOrder()))
          .toList();
      int btcRank = sortedSymbolInfos.size();
      for (int i = 0; i < sortedSymbolInfos.size(); i++) {
        YamaDateSymbolInfo yamaDateSymbolInfo = sortedSymbolInfos.get(i);
        if (StringUtils.equals(yamaDateSymbolInfo.getSymbol(), YAMA_01_ALT_COIN_STANDARD_SYMBOL)) {
          btcRank = i + 1;
          break;
        }
      }
      float indexValue = (float) btcRank / sortedSymbolInfos.size();
      index.setBtcRank(btcRank);
      index.setIndex(indexValue);
      dateIndexMap.put(openTime, index);
    });
    return dateIndexMap;
  }

  private NavigableMap<Long, Yama02Index> calculateYama02DateIndexMap(List<String> symbols, Long klineStartDate, Long klineEndDate) {
    NavigableMap<Long, Yama02Index> dateIndexMap = new TreeMap<>();
    Map<Long, Map<String, YamaDateSymbolInfo>> dateSymbolInfos = calculateDateSymbolInfos(symbols, klineStartDate, klineEndDate);
    dateSymbolInfos.forEach(
        (openTime, symbolInfos) -> {
          Yama02Index index = new Yama02Index();
          index.setOpenTime(openTime);
          index.setSymbolCount(symbolInfos.size());
          List<YamaDateSymbolInfo> sortedSymbolInfos =
              symbolInfos.values().stream()
                  .sorted(
                      Comparator.comparing(
                          YamaDateSymbolInfo::getQuoteVolumeSum, Comparator.reverseOrder()))
                  .limit(yama02altCoinIndexQuoteVolumeMaxRank)
                  .toList();
          float totalPctChangeSum =
              sortedSymbolInfos.stream()
                  .map(YamaDateSymbolInfo::getPctChange)
                  .reduce(0f, Float::sum);
          float indexValue = totalPctChangeSum / sortedSymbolInfos.size();
          index.setPctChangeSum(totalPctChangeSum);
          index.setIndex(indexValue);
          dateIndexMap.put(openTime, index);
        });
    return dateIndexMap;
  }

  private static List<BigDecimal> calculateRollingMeans(List<BigDecimal> values, int windowSize) {
    List<BigDecimal> result = new ArrayList<>();

    for (int i = 0; i < values.size(); i++) {
      int start = Math.max(0, i - windowSize + 1);
      int end = i + 1;

      BigDecimal sum = BigDecimal.ZERO;
      for (int j = start; j < end; j++) {
        sum = sum.add(values.get(j));
      }
      BigDecimal mean = sum.divide(new BigDecimal(end - start), Constants.SCALE, RoundingMode.DOWN);
      result.add(mean);
    }

    return result;
  }
}
