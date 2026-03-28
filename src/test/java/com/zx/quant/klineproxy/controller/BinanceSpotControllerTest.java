package com.zx.quant.klineproxy.controller;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zx.quant.klineproxy.config.GlobalExceptionConfig;
import com.zx.quant.klineproxy.config.SerializeConfig;
import com.zx.quant.klineproxy.client.BinanceSpotClient;
import com.zx.quant.klineproxy.client.model.BinanceSpotExchange;
import com.zx.quant.klineproxy.client.model.BinanceSpotSymbol;
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
import retrofit2.Call;
import retrofit2.Response;

@WebMvcTest(BinanceSpotController.class)
@Import({SerializeConfig.class, GlobalExceptionConfig.class})
class BinanceSpotControllerTest {

  @Autowired
  private MockMvc mockMvc;

  @MockBean(name = "binanceSpotKlineService")
  private KlineService klineService;

  @MockBean
  private ExchangeService<?> exchangeService;

  @MockBean
  private BinanceSpotClient binanceSpotClient;

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

  @Test
  void shouldRejectCsvSymbolsPayload() throws Exception {
    mockMvc.perform(get("/api/v3/ticker/24hr").param("symbols", "BTCUSDT,ETHUSDT"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(-1100));
  }

  @Test
  void shouldReturnMiniTickerWhenTypeMini() throws Exception {
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol("BTCUSDT");
    ticker24Hr.setOpenPrice(new BigDecimal("90"));
    ticker24Hr.setHighPrice(new BigDecimal("110"));
    ticker24Hr.setLowPrice(new BigDecimal("80"));
    ticker24Hr.setLastPrice(new BigDecimal("100"));
    ticker24Hr.setVolume(new BigDecimal("12"));
    ticker24Hr.setQuoteVolume(new BigDecimal("1200"));
    ticker24Hr.setOpenTime(1L);
    ticker24Hr.setCloseTime(2L);
    ticker24Hr.setFirstId(3L);
    ticker24Hr.setLastId(4L);
    ticker24Hr.setCount(5L);
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(klineService.queryTicker24hrs(List.of("BTCUSDT"))).willReturn(List.of(ticker24Hr));

    mockMvc.perform(get("/api/v3/ticker/24hr")
            .param("symbol", "BTCUSDT")
            .param("type", "MINI"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$.openPrice").value("90"))
        .andExpect(jsonPath("$.lastPrice").value("100"))
        .andExpect(jsonPath("$.priceChange").doesNotExist());
  }

  @Test
  void shouldRejectSingleSymbolStatusMismatch() throws Exception {
    BinanceSpotExchange exchange = new BinanceSpotExchange();
    BinanceSpotSymbol symbol = new BinanceSpotSymbol();
    symbol.setSymbol("BTCUSDT");
    symbol.setStatus("HALT");
    exchange.setSymbols(List.of(symbol));
    given(exchangeService.queryExchange()).willReturn(exchange);

    mockMvc.perform(get("/api/v3/ticker/24hr")
            .param("symbol", "BTCUSDT")
            .param("symbolStatus", "TRADING"))
        .andExpect(status().isBadRequest())
        .andExpect(jsonPath("$.code").value(-1220));
  }

  @Test
  void shouldFilterSymbolsBySymbolStatus() throws Exception {
    BinanceSpotExchange exchange = new BinanceSpotExchange();
    BinanceSpotSymbol btc = new BinanceSpotSymbol();
    btc.setSymbol("BTCUSDT");
    btc.setStatus("TRADING");
    BinanceSpotSymbol eth = new BinanceSpotSymbol();
    eth.setSymbol("ETHUSDT");
    eth.setStatus("HALT");
    exchange.setSymbols(List.of(btc, eth));
    Ticker24Hr ticker24Hr = new Ticker24Hr();
    ticker24Hr.setSymbol("BTCUSDT");
    ticker24Hr.setLastPrice(new BigDecimal("100"));
    given(exchangeService.queryExchange()).willReturn(exchange);
    given(klineService.queryTicker24hrs(List.of("BTCUSDT"))).willReturn(List.of(ticker24Hr));

    mockMvc.perform(get("/api/v3/ticker/24hr")
            .param("symbols", "[\"BTCUSDT\",\"ETHUSDT\"]")
            .param("symbolStatus", "TRADING"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].symbol").value("BTCUSDT"))
        .andExpect(jsonPath("$[1]").doesNotExist());
    verify(klineService).queryTicker24hrs(List.of("BTCUSDT"));
  }

  @Test
  void shouldProxyTimezoneKlinesToOfficialClient() throws Exception {
    @SuppressWarnings("unchecked")
    Call<List<Object[]>> call = (Call<List<Object[]>>) org.mockito.Mockito.mock(Call.class);
    List<Object[]> klines = java.util.Collections.singletonList(new Object[] {1, "2", "3"});
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(binanceSpotClient.getKlines("BTCUSDT", "1d", null, null, 500, "8")).willReturn(call);
    given(call.execute()).willReturn(Response.success(klines));

    mockMvc.perform(get("/api/v3/klines")
            .param("symbol", "BTCUSDT")
            .param("interval", "1d")
            .param("timeZone", "8"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0][0]").value(1))
        .andExpect(jsonPath("$[0][1]").value("2"));
  }

  @Test
  void shouldProxyMonthlyKlinesToOfficialClient() throws Exception {
    @SuppressWarnings("unchecked")
    Call<List<Object[]>> call = (Call<List<Object[]>>) org.mockito.Mockito.mock(Call.class);
    List<Object[]> klines = java.util.Collections.singletonList(new Object[] {1, "2", "3"});
    given(exchangeService.querySymbols()).willReturn(List.of("BTCUSDT"));
    given(binanceSpotClient.getKlines("BTCUSDT", "1M", null, null, 500, null)).willReturn(call);
    given(call.execute()).willReturn(Response.success(klines));

    mockMvc.perform(get("/api/v3/klines")
            .param("symbol", "BTCUSDT")
            .param("interval", "1M"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0][0]").value(1))
        .andExpect(jsonPath("$[0][1]").value("2"));
  }
}
