package com.zx.quant.klineproxy.model;

/** Source precedence is used only for equal-version closed snapshots. */
public enum KlineUpdateSource {
  STREAM, REST, RESTORE, SYNTHETIC
}
