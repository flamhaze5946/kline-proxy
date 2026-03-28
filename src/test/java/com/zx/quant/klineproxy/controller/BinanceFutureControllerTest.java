package com.zx.quant.klineproxy.controller;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zx.quant.klineproxy.config.GlobalExceptionConfig;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.FuturePremiumIndex;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(BinanceFutureController.class)
@Import({SerializeConfig.class, GlobalExceptionConfig.class})
class BinanceFutureControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @MockBean(name = "binanceFutureKlineService")
  private KlineService klineService;

  @MockBean
  private FutureExchangeService<?> exchangeService;

  @Test
  void shouldIgnoreSymbolsParameterForTicker24hr() throws Exception {
    Ticker24Hr btcTicker = new Ticker24Hr();
    btcTicker.setSymbol("BTCUSDT");
    btcTicker.setLastPrice(new BigDecimal("100"));
    Ticker24Hr ethTicker = new Ticker24Hr();
    ethTicker.setSymbol("ETHUSDT");
    ethTicker.setLastPrice(new BigDecimal("200"));
    given(klineService.queryTicker24hrs(List.of())).willReturn(List.of(btcTicker, ethTicker));

    mockMvc.perform(get("/fapi/v1/ticker/24hr").param("symbols", "[\"BTCUSDT\"]"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$[1].symbol").value("ETHUSDT"));
  }

  @Test
  void shouldPreferSymbolOverSymbolsForTickerPrice() throws Exception {
    Ticker.BigDecimalTicker ticker = new Ticker.BigDecimalTicker();
    ticker.setSymbol("BTCUSDT");
    ticker.setPrice(new BigDecimal("100"));
    ticker.setTime(123L);
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT", "ETHUSDT"));
    given(klineService.queryTickers(List.of("BTCUSDT"))).willReturn(List.of(ticker));

    mockMvc.perform(get("/fapi/v1/ticker/price")
            .param("symbol", "BTCUSDT")
            .param("symbols", "[\"ETHUSDT\"]"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$.price").value("100"));
  }

  @Test
  void shouldReturnFundingRateFieldsAsStringsAndPassParams() throws Exception {
    FutureFundingRate fundingRate = new FutureFundingRate();
    fundingRate.setSymbol("BTCUSDT");
    fundingRate.setFundingTime(1L);
    fundingRate.setFundingRate(new BigDecimal("0.0001"));
    fundingRate.setMarkPrice(new BigDecimal("100"));
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(exchangeService.queryFundingRates("BTCUSDT", 10L, 20L, 30))
        .willReturn(List.of(fundingRate));

    mockMvc.perform(get("/fapi/v1/fundingRate")
            .param("symbol", "BTCUSDT")
            .param("startTime", "10")
            .param("endTime", "20")
            .param("limit", "30"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$[0].fundingRate").value("0.0001"))
        .andExpect(jsonPath("$[0].markPrice").value("100"));
    verify(exchangeService).queryFundingRates("BTCUSDT", 10L, 20L, 30);
  }

  @Test
  void shouldReturnPremiumIndexFieldsAsStrings() throws Exception {
    FuturePremiumIndex premiumIndex = new FuturePremiumIndex();
    premiumIndex.setSymbol("BTCUSDT");
    premiumIndex.setMarkPrice(new BigDecimal("100"));
    premiumIndex.setIndexPrice(new BigDecimal("101"));
    premiumIndex.setEstimatedSettlePrice(new BigDecimal("102"));
    premiumIndex.setLastFundingRate(new BigDecimal("0.0001"));
    premiumIndex.setInterestRate(new BigDecimal("0.0002"));
    premiumIndex.setNextFundingTime(2L);
    premiumIndex.setTime(3L);
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(exchangeService.queryPremiumIndex("BTCUSDT")).willReturn(premiumIndex);

    mockMvc.perform(get("/fapi/v1/premiumIndex").param("symbol", "BTCUSDT"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$.markPrice").value("100"))
        .andExpect(jsonPath("$.indexPrice").value("101"))
        .andExpect(jsonPath("$.lastFundingRate").value("0.0001"))
        .andExpect(jsonPath("$.interestRate").value("0.0002"));
  }
}
