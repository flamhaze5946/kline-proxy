package com.zx.quant.klineproxy.service.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.zx.quant.klineproxy.client.model.BinanceFutureExchange;
import com.zx.quant.klineproxy.client.model.BinanceFutureSymbol;
import com.zx.quant.klineproxy.model.EventKlineEvent;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/** Futures continuous-stream protocol and its pair/contract-to-symbol mapping. */
public final class BinanceContinuousKlineStream extends AbstractBinanceKlineStream {

  // A rolling quarterly series is not a single dated contract's history.
  private static final Set<String> CONTRACT_TYPES = Set.of("PERPETUAL", "TRADIFI_PERPETUAL");

  private final Supplier<BinanceFutureExchange> exchangeSupplier;

  private volatile Routes routes = new Routes(List.of(), Map.of(), Map.of());

  public BinanceContinuousKlineStream(Supplier<BinanceFutureExchange> exchangeSupplier) {
    this.exchangeSupplier = exchangeSupplier;
  }

  @Override
  public String eventType() {
    return "continuous_kline";
  }

  @Override
  public String subscriptionTopic(String symbol, String interval) {
    Contract contract = currentRoutes().bySymbol().get(symbol);
    return contract != null ? contract.topic(interval) : null;
  }

  @Override
  protected String resolveSymbol(EventKlineEvent<?, ?> event) {
    if (StringUtils.isAnyBlank(event.getPair(), event.getContractType())) {
      return null;
    }
    return currentRoutes().byContract().get(new Contract(event.getPair(), event.getContractType()));
  }

  @Override
  protected String rawTopic(JsonNode payload) {
    String pair = text(payload, "ps");
    String contractType = text(payload, "ct");
    String interval = interval(payload);
    if (StringUtils.isAnyBlank(pair, contractType, interval) || !CONTRACT_TYPES.contains(contractType)) {
      return null;
    }
    return new Contract(pair, contractType).topic(interval);
  }

  @Override
  public Object subscriptionState() {
    return currentRoutes().bySymbol();
  }

  private Routes currentRoutes() {
    BinanceFutureExchange exchange = exchangeSupplier.get();
    List<BinanceFutureSymbol> symbols = exchange != null && exchange.getSymbols() != null
        ? exchange.getSymbols() : List.of();
    Routes snapshot = routes;
    // Refresh replaces the exchange's deserialized list; do not scan all symbols
    // on every streaming update. The published lookup maps are immutable.
    if (snapshot.exchangeSymbols() == symbols) {
      return snapshot;
    }
    Map<Contract, List<BinanceFutureSymbol>> candidates = symbols.stream()
        .filter(symbol -> "TRADING".equals(symbol.getStatus()))
        .filter(symbol -> StringUtils.isNoneBlank(symbol.getSymbol(), symbol.getPair(), symbol.getContractType()))
        .filter(symbol -> CONTRACT_TYPES.contains(symbol.getContractType()))
        .collect(Collectors.groupingBy(symbol -> new Contract(symbol.getPair(), symbol.getContractType())));
    Map<String, Contract> bySymbol = new HashMap<>();
    Map<Contract, String> byContract = new HashMap<>();
    candidates.forEach((contract, matches) -> {
      if (matches.size() == 1) {
        String symbol = matches.getFirst().getSymbol();
        bySymbol.put(symbol, contract);
        byContract.put(contract, symbol);
      }
    });
    Routes refreshed = new Routes(symbols, Map.copyOf(bySymbol), Map.copyOf(byContract));
    routes = refreshed;
    return refreshed;
  }

  private record Contract(String pair, String contractType) {
    private String topic(String interval) {
      return pair.toLowerCase(Locale.ROOT) + "_" + contractType.toLowerCase(Locale.ROOT)
          + "@continuousKline_" + interval;
    }
  }

  private record Routes(List<BinanceFutureSymbol> exchangeSymbols,
                        Map<String, Contract> bySymbol,
                        Map<Contract, String> byContract) {
  }
}
