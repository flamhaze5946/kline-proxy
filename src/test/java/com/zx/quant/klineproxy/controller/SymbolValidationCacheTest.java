package com.zx.quant.klineproxy.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.zx.quant.klineproxy.client.model.*;
import com.zx.quant.klineproxy.model.exceptions.ApiException;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class SymbolValidationCacheTest {
  @Test
  void futureValidationIncludesNonTradingAndChangesWithExchangePublication() {
    var controller = new BinanceFutureController();
    FutureExchangeService<BinanceFutureExchange> exchange = mock(FutureExchangeService.class);
    ReflectionTestUtils.setField(controller, "exchangeService", exchange);
    ReflectionTestUtils.setField(controller, "klineService", mock(KlineService.class));
    BinanceFutureSymbol old = new BinanceFutureSymbol(); old.setSymbol("OLDUSDT"); old.setStatus("SETTLING");
    BinanceFutureExchange first = new BinanceFutureExchange(); first.setSymbols(List.of(old));
    when(exchange.queryExchange()).thenReturn(first);
    assertDoesNotThrow(() -> controller.queryTicker("OLDUSDT"));
    first.setServerTime(123L);
    assertDoesNotThrow(() -> controller.queryTicker("OLDUSDT"));
    BinanceFutureSymbol fresh = new BinanceFutureSymbol(); fresh.setSymbol("NEWUSDT"); fresh.setStatus("TRADING");
    BinanceFutureExchange second = new BinanceFutureExchange(); second.setSymbols(List.of(fresh));
    when(exchange.queryExchange()).thenReturn(second);
    assertThrows(ApiException.class, () -> controller.queryTicker("OLDUSDT"));
    assertDoesNotThrow(() -> controller.queryTicker("NEWUSDT"));
  }

  @Test
  void spotValidationAndStatusFilterFollowTheCurrentPublication() {
    var controller = new BinanceSpotController();
    ExchangeService<BinanceSpotExchange> exchange = mock(ExchangeService.class);
    ReflectionTestUtils.setField(controller, "exchangeService", exchange);
    ReflectionTestUtils.setField(controller, "klineService", mock(KlineService.class));
    BinanceSpotSymbol halted = new BinanceSpotSymbol(); halted.setSymbol("BTCUSDT"); halted.setStatus("HALT");
    BinanceSpotExchange first = new BinanceSpotExchange(); first.setSymbols(List.of(halted));
    when(exchange.queryExchange()).thenReturn(first);
    assertDoesNotThrow(() -> controller.queryTicker("BTCUSDT", null));
    assertDoesNotThrow(() -> controller.queryTicker24Hr("BTCUSDT", null, "FULL", "HALT"));
    assertThrows(ApiException.class, () -> controller.queryTicker24Hr("BTCUSDT", null, "FULL", "TRADING"));
    BinanceSpotSymbol trading = new BinanceSpotSymbol(); trading.setSymbol("BTCUSDT"); trading.setStatus("TRADING");
    BinanceSpotExchange second = new BinanceSpotExchange(); second.setSymbols(List.of(trading));
    when(exchange.queryExchange()).thenReturn(second);
    assertDoesNotThrow(() -> controller.queryTicker24Hr("BTCUSDT", null, "FULL", "TRADING"));
    assertThrows(ApiException.class, () -> controller.queryTicker24Hr("BTCUSDT", null, "FULL", "HALT"));
  }
}
