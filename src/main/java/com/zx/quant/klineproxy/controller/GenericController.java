package com.zx.quant.klineproxy.controller;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;

/**
 * generic controller
 * @author flamhaze5946
 */
public class GenericController {
  protected List<String> getRealSymbols(String symbol, List<String> symbols) {
    List<String> realSymbols = new ArrayList<>();
    if (symbol != null) {
      realSymbols.add(symbol);
    }
    if (CollectionUtils.isNotEmpty(symbols)) {
      realSymbols.addAll(symbols);
    }
    realSymbols = realSymbols.stream()
        .distinct()
        .toList();
    return realSymbols;
  }
}
