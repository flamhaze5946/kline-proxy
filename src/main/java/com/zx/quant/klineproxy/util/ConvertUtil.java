package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.collections4.CollectionUtils;

/**
 * convert util
 * @author flamhaze5946
 */
public final class ConvertUtil {

  private static final String DOUBLE_FORMAT = "%.8f";

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

  public static Object convertToDisplayTicker(Ticker<?> ticker) {
    if (ticker == null) {
      return new Object[0];
    }
    DisplayTicker displayTicker = new DisplayTicker();
    displayTicker.setSymbol(ticker.getSymbol());
    displayTicker.setTime(ticker.getTime());
    if (ticker instanceof Ticker.DoubleTicker doubleTicker) {
      displayTicker.setPrice(formatForDouble(doubleTicker.getPrice()));
    } else if(ticker instanceof Ticker.BigDecimalTicker bigDecimalTicker){
      displayTicker.setPrice(bigDecimalTicker.getPrice().toPlainString());
    }
    return displayTicker;
  }

  public static Object[] convertToDisplayKline(Kline<?> kline) {
    if (kline == null) {
      return new Object[0];
    }

    if (kline instanceof Kline.DoubleKline doubleKline) {
      return new Object[] {
          doubleKline.getOpenTime(),
          formatForDouble(doubleKline.getOpenPrice()),
          formatForDouble(doubleKline.getHighPrice()),
          formatForDouble(doubleKline.getLowPrice()),
          formatForDouble(doubleKline.getClosePrice()),
          formatForDouble(doubleKline.getVolume()),
          doubleKline.getCloseTime(),
          formatForDouble(doubleKline.getQuoteVolume()),
          doubleKline.getTradeNum(),
          formatForDouble(doubleKline.getActiveBuyVolume()),
          formatForDouble(doubleKline.getActiveBuyQuoteVolume()),
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

  private static String formatForDouble(Double number) {
    if (number == null) {
      return null;
    }
    return String.format(DOUBLE_FORMAT, number);
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  private static class DisplayTicker extends Ticker<String> {
  }
}
