package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.model.constant.Constants;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import retrofit2.Call;
import retrofit2.Response;

class BinanceFutureExchangeServiceImplTest {

  @Test
  void queryBulkFundingRatesShouldUseRecentCacheForIdenticalLiveRequests() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    RateLimitManager rateLimitManager = mock(RateLimitManager.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", rateLimitManager);

    FutureFundingRate fundingRate = new FutureFundingRate();
    fundingRate.setSymbol("BTCUSDT");
    fundingRate.setFundingTime(1L);
    fundingRate.setFundingRate(new BigDecimal("0.0001"));
    fundingRate.setMarkPrice(new BigDecimal("100"));
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates("BTCUSDT", null, null, 1)).willReturn(call);
    given(call.execute()).willReturn(Response.success(List.of(fundingRate)));

    var first = service.queryBulkFundingRates(List.of("BTCUSDT"), null, null, 1);
    var second = service.queryBulkFundingRates(List.of("BTCUSDT"), null, null, 1);

    assertEquals("0.0001", first.fundingRates().get("BTCUSDT").get(0).getFundingRate());
    assertEquals(first.fundingRates(), second.fundingRates());
    verify(client, times(1)).getFundingRates("BTCUSDT", null, null, 1);
    verify(rateLimitManager, times(1))
        .acquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 5);
  }

  @Test
  void queryBulkFundingRatesShouldRouteShortWindowSymbolsThroughChunkCache() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    RateLimitManager rateLimitManager = mock(RateLimitManager.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", rateLimitManager);

    FutureFundingRate fundingRate = new FutureFundingRate();
    fundingRate.setSymbol("BTCUSDT");
    fundingRate.setFundingTime(10L);
    fundingRate.setFundingRate(new BigDecimal("0.0001"));
    fundingRate.setMarkPrice(new BigDecimal("100"));
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> noSymbolCall = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(isNull(), any(Long.class), any(Long.class), eq(1000)))
        .willReturn(noSymbolCall);
    given(noSymbolCall.execute()).willReturn(Response.success(List.of(fundingRate)));

    long sinceMs = 0L;
    long untilMs = 60L * 60L * 1000L;
    var resp = service.queryBulkFundingRates(
        List.of("BTCUSDT", "ETHUSDT"), sinceMs, untilMs, 5);

    assertTrue(resp.fundingRates().containsKey("BTCUSDT"),
        "BTCUSDT must appear in chunk-cache filtered response");
    assertTrue(resp.fundingRates().containsKey("ETHUSDT"),
        "ETHUSDT must appear in chunk-cache response even if no rows");
    assertEquals("0.0001",
        resp.fundingRates().get("BTCUSDT").get(0).getFundingRate(),
        "BTCUSDT row should carry the chunk's fundingRate");
    assertTrue(resp.fundingRates().get("ETHUSDT").isEmpty(),
        "ETHUSDT should have empty row list — chunk had no ETHUSDT event");

    verify(client, never()).getFundingRates(eq("BTCUSDT"), any(), any(), any());
    verify(client, never()).getFundingRates(eq("ETHUSDT"), any(), any(), any());
    verify(client, times(1))
        .getFundingRates(isNull(), any(Long.class), any(Long.class), eq(1000));
  }
}
