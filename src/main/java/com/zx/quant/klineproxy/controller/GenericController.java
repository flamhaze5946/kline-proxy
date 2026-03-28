package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

/**
 * generic controller
 * @author flamhaze5946
 */
public class GenericController {

  @Autowired
  protected Serializer serializer;

  protected List<String> getRealSymbols(String symbol, String symbolsPayload) {
    if (StringUtils.isNotBlank(symbol) && StringUtils.isNotBlank(symbolsPayload)) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1128,
          "Combination of optional parameters invalid. Recommendation: 'symbol' and 'symbols' cannot both be sent.");
    }

    LinkedHashSet<String> realSymbols = new LinkedHashSet<>();
    if (StringUtils.isNotBlank(symbol)) {
      realSymbols.add(symbol);
    }

    if (StringUtils.isBlank(symbolsPayload)) {
      return new ArrayList<>(realSymbols);
    }

    if (StringUtils.startsWith(StringUtils.trim(symbolsPayload), "[")) {
      try {
        String[] symbols = serializer.fromJsonString(symbolsPayload, String[].class);
        if (symbols == null) {
          throw invalidSymbolsParameter();
        }
        for (String item : symbols) {
          if (StringUtils.isNotBlank(item)) {
            realSymbols.add(item);
          }
        }
      } catch (RuntimeException e) {
        throw invalidSymbolsParameter();
      }
    } else {
      for (String item : StringUtils.split(symbolsPayload, ",")) {
        if (StringUtils.isNotBlank(item)) {
          realSymbols.add(StringUtils.trim(item));
        }
      }
    }

    return new ArrayList<>(realSymbols);
  }

  protected boolean shouldReturnArray(String symbol) {
    return StringUtils.isBlank(symbol);
  }

  protected void validateSymbols(Collection<String> symbols, Collection<String> validSymbols) {
    if (CollectionUtils.isEmpty(symbols)) {
      return;
    }
    for (String requestSymbol : symbols) {
      if (!validSymbols.contains(requestSymbol)) {
        throw new ApiException(HttpStatus.BAD_REQUEST, -1121, "Invalid symbol.");
      }
    }
  }

  private ApiException invalidSymbolsParameter() {
    return new ApiException(HttpStatus.BAD_REQUEST, -1100, "Invalid symbols parameter.");
  }
}
