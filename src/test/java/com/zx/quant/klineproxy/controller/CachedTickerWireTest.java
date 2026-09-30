package com.zx.quant.klineproxy.controller;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.zx.quant.klineproxy.client.model.*;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.Serializer;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

class CachedTickerWireTest {
  @Test
  void cachedBytesKeepTheOriginalJsonWireFormatForEveryAllMarketView() throws Exception {
    var serializer = new Serializer(new SerializeConfig().objectMapper());
    var future = new BinanceFutureController();
    var spot = new BinanceSpotController();
    var service = mock(KlineService.class);
    var futureExchange = mock(FutureExchangeService.class);
    var spotExchange = mock(ExchangeService.class);
    BinanceSpotExchange spotMetadata = new BinanceSpotExchange();
    spotMetadata.setSymbols(List.of());
    when(spotExchange.queryExchange()).thenReturn(spotMetadata);
    when(futureExchange.querySymbols()).thenReturn(List.of());
    when(spotExchange.querySymbols()).thenReturn(List.of());
    ReflectionTestUtils.setField(future, "exchangeService", futureExchange);
    ReflectionTestUtils.setField(spot, "exchangeService", spotExchange);
    for (Object controller : List.of(future, spot)) {
      ReflectionTestUtils.setField(controller, "klineService", service);
      ReflectionTestUtils.setField(controller, "serializer", serializer);
    }
    var ticker = new Ticker.BigDecimalTicker();
    ticker.setSymbol("测试USDT"); ticker.setPrice(new BigDecimal("100.12340000")); ticker.setTime(123L);
    List<Ticker<?>> prices = List.of(ticker);
    var daily = new Ticker24Hr(); daily.setSymbol("测试USDT");
    daily.setLastPrice(new BigDecimal("0.0000000100")); daily.setCount(2L);
    List<Ticker24Hr> stats = List.of(daily);
    when(service.queryTickers(any())).thenReturn(prices);
    when(service.queryTicker24hrs(any())).thenReturn(stats);
    var mvc = MockMvcBuilders.standaloneSetup(future, spot).build();
    Object[][] cases = {
        {"/fapi/v1/ticker/price", ConvertUtil.convertToDisplayTicker(prices, true)},
        {"/api/v3/ticker/price", ConvertUtil.convertToDisplayTicker(prices, true, false)},
        {"/fapi/v1/ticker/24hr", ConvertUtil.convertToDisplayTicker24hr(stats, true)},
        {"/api/v3/ticker/24hr", ConvertUtil.convertToDisplayTicker24hr(stats, true, false)},
        {"/api/v3/ticker/24hr?type=MINI", ConvertUtil.convertToDisplayTicker24hr(stats, true, true)}
    };
    for (Object[] test : cases) {
      byte[] original = serializer.toJsonString(test[1]).getBytes(StandardCharsets.UTF_8);
      for (int pass = 0; pass < 2; pass++) {
        mvc.perform(get((String) test[0])).andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andExpect(content().bytes(original));
      }
    }
    assertSame(future.queryTicker(null), future.queryTicker(null));
    var replacement = new Ticker.BigDecimalTicker(); replacement.setSymbol("BTCUSDT");
    replacement.setPrice(new BigDecimal("200.00")); replacement.setTime(456L);
    when(service.queryTickers(any())).thenReturn(List.of(replacement));
    mvc.perform(get("/fapi/v1/ticker/price")).andExpect(status().isOk())
        .andExpect(jsonPath("$[0].price").value("200.00"));
  }
}
