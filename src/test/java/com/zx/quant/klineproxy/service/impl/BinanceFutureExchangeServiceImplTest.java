package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
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
        .acquire(Constants.BINANCE_FUTURE_KLINES_FETCHER_RATE_LIMITER_NAME, 1);
  }
}
