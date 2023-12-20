package com.zx.quant.klineproxy.model;

import com.zx.quant.klineproxy.model.EventKline.BigDecimalEventKline;
import com.zx.quant.klineproxy.model.EventKline.DoubleEventKline;
import java.math.BigDecimal;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * combine kline event
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public abstract class CombineKlineEvent<N, EKN extends EventKline<N>> extends CombineEvent<EventKlineEvent<N, EKN>> {

  @EqualsAndHashCode(callSuper = true)
  @Data
  public static class DoubleCombineKlineEvent extends CombineKlineEvent<Double, DoubleEventKline> {
  }

  @EqualsAndHashCode(callSuper = true)
  @Data
  public static class BigDecimalCombineKlineEvent extends CombineKlineEvent<BigDecimal, BigDecimalEventKline> {
  }
}
