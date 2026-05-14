package com.zx.quant.klineproxy.controller;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zx.quant.klineproxy.config.GlobalExceptionConfig;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.model.BulkFundingRateResponse;
import com.zx.quant.klineproxy.model.BulkKlinesResponse;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.FuturePremiumIndex;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.model.Ticker;
import com.zx.quant.klineproxy.model.Ticker24Hr;
import com.zx.quant.klineproxy.service.FutureExchangeService;
import com.zx.quant.klineproxy.service.KlineService;
import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
  void shouldReturnAllMarketTickerPriceArrayWithoutSymbol() throws Exception {
    Ticker.BigDecimalTicker btcTicker = new Ticker.BigDecimalTicker();
    btcTicker.setSymbol("BTCUSDT");
    btcTicker.setPrice(new BigDecimal("100"));
    btcTicker.setTime(123L);
    Ticker.BigDecimalTicker ethTicker = new Ticker.BigDecimalTicker();
    ethTicker.setSymbol("ETHUSDT");
    ethTicker.setPrice(new BigDecimal("200"));
    ethTicker.setTime(456L);
    given(klineService.queryTickers(List.of())).willReturn(List.of(btcTicker, ethTicker));

    mockMvc.perform(get("/fapi/v1/ticker/price"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$[1].symbol").value("ETHUSDT"));
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
  void shouldReturnBulkKlinesAndPassNormalizedSymbols() throws Exception {
    Map<String, List<Object[]>> rows = new LinkedHashMap<>();
    rows.put("BTCUSDT", java.util.Collections.singletonList(new Object[] {1L, "2", "3", "4", "5", "6", 7L, "8", 9, "10", "11", "0"}));
    given(klineService.queryBulkKlines("1h", 5, true, List.of("BTCUSDT", "ETHUSDT")))
        .willReturn(new BulkKlinesResponse("1h", 123L, rows));

    mockMvc.perform(get("/fapi/v1/klines/bulk")
            .param("interval", "1h")
            .param("limit", "5")
            .param("closed_only", "true")
            .param("symbols", "ETHUSDT,BTCUSDT"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.interval").value("1h"))
        .andExpect(jsonPath("$.ts_ms").value(123L))
        .andExpect(jsonPath("$.klines.BTCUSDT[0][0]").value(1L))
        .andExpect(jsonPath("$.klines.BTCUSDT[0][11]").value("0"));
    verify(klineService).queryBulkKlines("1h", 5, true, List.of("BTCUSDT", "ETHUSDT"));
  }

  @Test
  void shouldReturnBulkFundingRateAndPassParams() throws Exception {
    FutureFundingRate fundingRate = new FutureFundingRate();
    fundingRate.setSymbol("BTCUSDT");
    fundingRate.setFundingTime(1L);
    fundingRate.setFundingRate(new BigDecimal("0.0001"));
    fundingRate.setMarkPrice(new BigDecimal("100"));
    Map<String, List<ConvertUtil.DisplayFundingRate>> rows = new LinkedHashMap<>();
    rows.put("BTCUSDT", ConvertUtil.convertToDisplayFundingRates(List.of(fundingRate)));
    given(exchangeService.queryBulkFundingRates(List.of("BTCUSDT"), 10L, 20L, 3))
        .willReturn(new BulkFundingRateResponse(123L, rows));

    mockMvc.perform(get("/fapi/v1/fundingRate/bulk")
            .param("symbols", "BTCUSDT")
            .param("since_ms", "10")
            .param("until_ms", "20")
            .param("limit", "3"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.ts_ms").value(123L))
        .andExpect(jsonPath("$.fundingRates.BTCUSDT[0].fundingRate").value("0.0001"))
        .andExpect(jsonPath("$.fundingRates.BTCUSDT[0].markPrice").value("100"));
    verify(exchangeService).queryBulkFundingRates(List.of("BTCUSDT"), 10L, 20L, 3);
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
