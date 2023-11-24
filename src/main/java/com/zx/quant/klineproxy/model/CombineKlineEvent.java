package com.zx.quant.klineproxy.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * combine kline event
 * @author flamhaze5946
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class CombineKlineEvent extends CombineEvent<EventKlineEvent> {
}
