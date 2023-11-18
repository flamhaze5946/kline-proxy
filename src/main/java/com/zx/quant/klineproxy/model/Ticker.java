package com.zx.quant.klineproxy.model;

import java.math.BigDecimal;

/**
 * kline
 * @author flamhaze5946
 */
public record Ticker(String symbol, BigDecimal price, Long time) {
}
