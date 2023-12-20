package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Kline.BigDecimalKline;
import com.zx.quant.klineproxy.model.Kline.DoubleKline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

/**
 * convert util
 * @author flamhaze5946
 */
public final class ConvertUtil {

  public static Object convertToDisplayTicker(List<Ticker<?>> tickers) {
    if (CollectionUtils.isEmpty(tickers)) {
      return Collections.emptyList();
    }
    if (tickers.size() == 1) {
      return tickers.get(0);
    } else {
      return tickers;
    }
  }

  public static Object[] convertToDisplayKline(Kline<?> kline) {
    if (kline == null) {
      return new Object[0];
    }

    if (kline instanceof Kline.BigDecimalKline bigDecimalKline) {
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
          bigDecimalKline.getActiveBuyVolume(),
          bigDecimalKline.getActiveBuyQuoteVolume(),
          bigDecimalKline.getIgnore()
      };
    } else if(kline instanceof Kline.DoubleKline doubleKline) {
      return new Object[] {
          doubleKline.getOpenTime(),
          doubleKline.getOpenPrice(),
          doubleKline.getHighPrice(),
          doubleKline.getLowPrice(),
          doubleKline.getClosePrice(),
          doubleKline.getVolume(),
          doubleKline.getCloseTime(),
          doubleKline.getQuoteVolume(),
          doubleKline.getTradeNum(),
          doubleKline.getActiveBuyVolume(),
          doubleKline.getActiveBuyQuoteVolume(),
          doubleKline.getIgnore()
      };
    } else {
      throw new UnsupportedOperationException();
    }
  }
}
