package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.util.Serializer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;

/**
 * generic controller
 * @author flamhaze5946
 */
public class GenericController {

  protected static final Set<String> VALID_SYMBOL_STATUSES = Set.of("TRADING", "HALT", "BREAK");

  protected static final Set<String> VALID_TICKER_TYPES = Set.of("FULL", "MINI");

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

    if (!StringUtils.startsWith(StringUtils.trim(symbolsPayload), "[")) {
      throw invalidSymbolsParameter();
    }
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
    return new ApiException(
        HttpStatus.BAD_REQUEST,
        -1100,
        "Illegal characters found in parameter 'symbols'; legal range is '^\\[(\"[\\w\\-._&&[^a-z]]{1,50}\"(,\"[\\w\\-._&&[^a-z]]{1,50}\"){0,}){0,1}\\]$'.");
  }

  protected void validateTickerType(String type) {
    if (StringUtils.isBlank(type)) {
      return;
    }
    if (!VALID_TICKER_TYPES.contains(type)) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1139, "Invalid ticker type.");
    }
  }

  protected void validateSymbolStatus(String symbolStatus) {
    if (StringUtils.isBlank(symbolStatus)) {
      return;
    }
    if (!VALID_SYMBOL_STATUSES.contains(symbolStatus)) {
      throw new ApiException(HttpStatus.BAD_REQUEST, -1122, "Invalid symbolStatus.");
    }
  }
}
