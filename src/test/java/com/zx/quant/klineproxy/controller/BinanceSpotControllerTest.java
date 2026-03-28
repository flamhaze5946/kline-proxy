package com.zx.quant.klineproxy.controller;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zx.quant.klineproxy.config.GlobalExceptionConfig;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.ExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(BinanceSpotController.class)
@Import({SerializeConfig.class, GlobalExceptionConfig.class})
class BinanceSpotControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @MockBean(name = "binanceSpotKlineService")
  private KlineService klineService;

  @MockBean
  private ExchangeService<?> exchangeService;

  @Test
  void shouldReturnArrayWhenSymbolsParameterContainsSingleEntry() throws Exception {
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol("BTCUSDT");
    ticker24Hr.setLastPrice(new BigDecimal("100"));
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(klineService.queryTicker24hrs(List.of("BTCUSDT"))).willReturn(List.of(ticker24Hr));

    mockMvc.perform(get("/api/v3/ticker/24hr").param("symbols", "[\"BTCUSDT\"]"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$[0].lastPrice").value("100"));
  }

  @Test
  void shouldRejectCombinationOfSymbolAndSymbols() throws Exception {
    mockMvc.perform(get("/api/v3/ticker/24hr")
            .param("symbol", "BTCUSDT")
            .param("symbols", "[\"ETHUSDT\"]"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(-1128));
  }

  @Test
  void shouldRejectInvalidSymbol() throws Exception {
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));

    mockMvc.perform(get("/api/v3/ticker/24hr").param("symbol", "ETHUSDT"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(-1121))
        .andExpect(jsonPath("$.msg").value("Invalid symbol."));
  }

  @Test
  void shouldNotExposeTimeForSpotTickerPrice() throws Exception {
    Ticker.BigDecimalTicker ticker = new Ticker.BigDecimalTicker();
    ticker.setSymbol("BTCUSDT");
    ticker.setPrice(new BigDecimal("100"));
    ticker.setTime(123L);
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(klineService.queryTickers(List.of("BTCUSDT"))).willReturn(List.of(ticker));

    mockMvc.perform(get("/api/v3/ticker/price").param("symbol", "BTCUSDT"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$.price").value("100"))
        .andExpect(jsonPath("$.time").doesNotExist());
  }
}
