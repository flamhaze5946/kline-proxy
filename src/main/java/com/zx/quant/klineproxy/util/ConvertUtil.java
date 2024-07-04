package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;

/**
 * convert util
 * @author flamhaze5946
 */
public final class ConvertUtil {

  private static final String FLOAT_FORMAT = "#.########";

  private static final String DOUBLE_FORMAT = "#.########";

  private static final ThreadLocal<DecimalFormat> FLOAT_FORMATTER = ThreadLocal.withInitial(
      () -> new DecimalFormat(DOUBLE_FORMAT));

  private static final ThreadLocal<DecimalFormat> DOUBLE_FORMATTER = ThreadLocal.withInitial(
      () -> new DecimalFormat(DOUBLE_FORMAT));

  public static Object convertToDisplayTicker(List<Ticker<?>> tickers) {
    if (CollectionUtils.isEmpty(tickers)) {
      return Collections.emptyList();
    }
    if (tickers.size() == 1) {
      return convertToDisplayTicker(tickers.get(0));
    } else {
      return tickers.stream()
          .map(ConvertUtil::convertToDisplayTicker)
          .collect(Collectors.toList());
    }
  }

  public static Object convertToDisplayTicker24hr(List<Ticker24Hr> ticker24Hrs) {
    if (CollectionUtils.isEmpty(ticker24Hrs)) {
      return Collections.emptyList();
    }
    if (ticker24Hrs.size() == 1) {
      return convertToDisplayTicker24hr(ticker24Hrs.get(0));
    } else {
      return ticker24Hrs.stream()
          .map(ConvertUtil::convertToDisplayTicker24hr)
          .collect(Collectors.toList());
    }
  }

  public static Object convertToDisplayTicker(Ticker<?> ticker) {
    if (ticker == null) {
      return new Object[0];
    }
    DisplayTicker displayTicker = new DisplayTicker();
    displayTicker.setSymbol(ticker.getSymbol());
    displayTicker.setTime(ticker.getTime());
    if (ticker instanceof Ticker.StringTicker stringTicker) {
      displayTicker.setPrice(stringTicker.getPrice());
    } else if (ticker instanceof Ticker.FloatTicker floatTicker) {
      displayTicker.setPrice(floatToString(floatTicker.getPrice()));
    } else if (ticker instanceof Ticker.DoubleTicker doubleTicker) {
      displayTicker.setPrice(doubleToString(doubleTicker.getPrice()));
    } else if(ticker instanceof Ticker.BigDecimalTicker bigDecimalTicker){
      displayTicker.setPrice(bigDecimalTicker.getPrice().toPlainString());
    }
    return displayTicker;
  }

  public static Object convertToDisplayTicker24hr(Ticker24Hr ticker24Hr) {
    if (ticker24Hr == null) {
      return new Object[0];
    }
    DisplayTicker24Hr displayTicker24Hr = new DisplayTicker24Hr();
    displayTicker24Hr.setSymbol(ticker24Hr.getSymbol());
    displayTicker24Hr.setPriceChange(decimalToString(ticker24Hr.getPriceChange()));
    displayTicker24Hr.setPriceChangePercent(decimalToString(ticker24Hr.getPriceChangePercent()));
    displayTicker24Hr.setWeightedAvgPrice(decimalToString(ticker24Hr.getWeightedAvgPrice()));
    displayTicker24Hr.setPrevClosePrice(decimalToString(ticker24Hr.getPrevClosePrice()));
    displayTicker24Hr.setLastPrice(decimalToString(ticker24Hr.getLastPrice()));
    displayTicker24Hr.setLastQty(decimalToString(ticker24Hr.getLastQty()));
    displayTicker24Hr.setBidPrice(decimalToString(ticker24Hr.getBidPrice()));
    displayTicker24Hr.setBidQty(decimalToString(ticker24Hr.getBidQty()));
    displayTicker24Hr.setAskPrice(decimalToString(ticker24Hr.getAskPrice()));
    displayTicker24Hr.setAskQty(decimalToString(ticker24Hr.getAskQty()));
    displayTicker24Hr.setOpenPrice(decimalToString(ticker24Hr.getOpenPrice()));
    displayTicker24Hr.setHighPrice(decimalToString(ticker24Hr.getHighPrice()));
    displayTicker24Hr.setLowPrice(decimalToString(ticker24Hr.getLowPrice()));
    displayTicker24Hr.setVolume(decimalToString(ticker24Hr.getVolume()));
    displayTicker24Hr.setQuoteVolume(decimalToString(ticker24Hr.getQuoteVolume()));
    displayTicker24Hr.setOpenTime(ticker24Hr.getOpenTime());
    displayTicker24Hr.setCloseTime(ticker24Hr.getCloseTime());
    displayTicker24Hr.setFirstId(ticker24Hr.getFirstId());
    displayTicker24Hr.setLastId(ticker24Hr.getLastId());
    displayTicker24Hr.setCount(ticker24Hr.getCount());

    return displayTicker24Hr;
  }

  public static Object[] convertToDisplayKline(Kline<?> kline) {
    if (kline == null) {
      return new Object[0];
    }

    if (kline instanceof Kline.StringKline stringKline) {
      return new Object[] {
          stringKline.getOpenTime(),
          stringKline.getOpenPrice(),
          stringKline.getHighPrice(),
          stringKline.getLowPrice(),
          stringKline.getClosePrice(),
          stringKline.getVolume(),
          stringKline.getCloseTime(),
          stringKline.getQuoteVolume(),
          stringKline.getTradeNum(),
          stringKline.getActiveBuyVolume(),
          stringKline.getActiveBuyQuoteVolume(),
          stringKline.getIgnore()
          };
    } else if (kline instanceof Kline.FloatKline floatKline) {
      return new Object[] {
          floatKline.getOpenTime(),
          floatToString(floatKline.getOpenPrice()),
          floatToString(floatKline.getHighPrice()),
          floatToString(floatKline.getLowPrice()),
          floatToString(floatKline.getClosePrice()),
          floatToString(floatKline.getVolume()),
          floatKline.getCloseTime(),
          floatToString(floatKline.getQuoteVolume()),
          floatKline.getTradeNum(),
          floatToString(floatKline.getActiveBuyVolume()),
          floatToString(floatKline.getActiveBuyQuoteVolume()),
          floatKline.getIgnore()
          };
    } else if (kline instanceof Kline.DoubleKline doubleKline) {
      return new Object[] {
          doubleKline.getOpenTime(),
          doubleToString(doubleKline.getOpenPrice()),
          doubleToString(doubleKline.getHighPrice()),
          doubleToString(doubleKline.getLowPrice()),
          doubleToString(doubleKline.getClosePrice()),
          doubleToString(doubleKline.getVolume()),
          doubleKline.getCloseTime(),
          doubleToString(doubleKline.getQuoteVolume()),
          doubleKline.getTradeNum(),
          doubleToString(doubleKline.getActiveBuyVolume()),
          doubleToString(doubleKline.getActiveBuyQuoteVolume()),
          doubleKline.getIgnore()
      };
    } else if(kline instanceof Kline.BigDecimalKline bigDecimalKline) {
      return new Object[] {
          bigDecimalKline.getOpenTime(),
          bigDecimalKline.getOpenPrice().toPlainString(),
          bigDecimalKline.getHighPrice().toPlainString(),
          bigDecimalKline.getLowPrice().toPlainString(),
          bigDecimalKline.getClosePrice().toPlainString(),
          bigDecimalKline.getVolume().toPlainString(),
          bigDecimalKline.getCloseTime(),
          bigDecimalKline.getQuoteVolume().toPlainString(),
          bigDecimalKline.getTradeNum(),
          bigDecimalKline.getActiveBuyVolume().toPlainString(),
          bigDecimalKline.getActiveBuyQuoteVolume().toPlainString(),
          bigDecimalKline.getIgnore()
      };
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static String floatToString(Float number) {
    return nullOrValue(number, num -> FLOAT_FORMATTER.get().format(num));
  }

  private static String doubleToString(Double number) {
    return nullOrValue(number, num -> DOUBLE_FORMATTER.get().format(num));
  }

  private static String decimalToString(BigDecimal bigDecimal) {
    return nullOrValue(bigDecimal, BigDecimal::toPlainString);
  }

  private static <T> T nullOrValue(T item) {
    return nullOrValue(item, Function.identity());
  }

  private static <T, R> R nullOrValue(T item, Function<T, R> transformer) {
    if (item == null) {
      return null;
    }
    return transformer.apply(item);
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  private static class DisplayTicker extends Ticker<String> {
  }

  @Data
  private static class DisplayTicker24Hr {
    private String symbol;

    private String priceChange;

    private String priceChangePercent;

    private String weightedAvgPrice;

    private String prevClosePrice;

    private String lastPrice;

    private String lastQty;

    private String bidPrice;

    private String bidQty;

    private String askPrice;

    private String askQty;

    private String openPrice;

    private String highPrice;

    private String lowPrice;

    private String volume;

    private String quoteVolume;

    private Long openTime;

    private Long closeTime;

    private Long firstId;

    private Long lastId;

    private Long count;
  }
}
