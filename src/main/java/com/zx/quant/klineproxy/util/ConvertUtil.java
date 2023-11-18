package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.Ticker;
import java.util.Collections;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

/**
 * convert util
 * @author flamhaze5946
 */
public final class ConvertUtil {

  public static Object convertToDisplayTicker(List<Ticker> tickers) {
    if (CollectionUtils.isEmpty(tickers)) {
      return Collections.emptyList();
    }
    if (tickers.size() == 1) {
      return tickers.get(0);
    } else {
      return tickers;
    }
  }

  public static Object[] convertToDisplayKline(Kline kline) {
    if (kline == null) {
      return new Object[0];
    }

    return new Object[] {
        kline.getOpenTime(),
        kline.getOpenPrice().toPlainString(),
        kline.getHighPrice().toPlainString(),
        kline.getLowPrice().toPlainString(),
        kline.getClosePrice().toPlainString(),
        kline.getVolume().toPlainString(),
        kline.getCloseTime(),
        kline.getQuoteVolume().toPlainString(),
        kline.getTradeNum(),
        kline.getActiveBuyVolume(),
        kline.getActiveBuyQuoteVolume(),
        kline.getIgnore()
    };
  }
}
