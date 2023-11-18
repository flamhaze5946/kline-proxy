package com.zx.quant.klineproxy.util;

import com.zx.quant.klineproxy.model.Kline;

/**
 * convert util
 * @author flamhaze5946
 */
public final class ConvertUtil {

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
