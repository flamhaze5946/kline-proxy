package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.zx.quant.klineproxy.client.BinanceFutureClient;
import com.zx.quant.klineproxy.manager.RateLimitManager;
import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate;
import com.github.benmanes.caffeine.cache.Cache;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import retrofit2.Call;
import retrofit2.Response;

class VirtualFundingCacheTest {
  @Test
  void boundedCacheHitRetainsCapturedChunksEvenIfEvictedDuringAssembly() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    @SuppressWarnings("unchecked")
    Cache<Long, Map<String, List<DisplayFundingRate>>> cache =
        (Cache<Long, Map<String, List<DisplayFundingRate>>>) ReflectionTestUtils.getField(service, "historicalChunkCache");
    long hour = 3_600_000L;
    long start = Math.floorDiv(System.currentTimeMillis(), hour) * hour - 2 * hour;
    AtomicBoolean assembledOnVirtual = new AtomicBoolean();
    DisplayFundingRate first = new DisplayFundingRate() {
      @Override public Long getFundingTime() {
        assembledOnVirtual.set(Thread.currentThread().isVirtual());
        cache.invalidateAll();
        return start;
      }
    };
    DisplayFundingRate second = new DisplayFundingRate();
    second.setFundingTime(start + hour);
    service.seedHistoricalChunk(start, Map.of("DELISTEDUSDT", List.of(first)));
    service.seedHistoricalChunk(start + hour, Map.of("DELISTEDUSDT", List.of(second)));
    try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
      // All-symbol queries must include the historical chunk's symbols, without consulting
      // today's exchange metadata. Eviction must not send a captured hit back to REST.
      var result = requests.submit(() -> service.queryBulkFundingRates(null,
          start, start + 2 * hour, 1)).get(3, TimeUnit.SECONDS);
      assertTrue(assembledOnVirtual.get());
      assertEquals(Map.of("DELISTEDUSDT", List.of(second)), result.fundingRates());
      verifyNoInteractions(client);
    } finally {
      service.closeFundingLoads();
    }
  }

  @Test
  void coldVirtualRequestLoadsOnPlatformAndTheNextRequestUsesSameCache() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));
    FutureFundingRate rate = new FutureFundingRate();
    rate.setSymbol("BTCUSDT"); rate.setFundingTime(1L);
    rate.setFundingRate(new BigDecimal("0.0001")); rate.setMarkPrice(new BigDecimal("100"));
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = mock(Call.class);
    AtomicReference<Thread> loadingThread = new AtomicReference<>();
    when(client.getFundingRates("BTCUSDT", null, null, 1)).thenReturn(call);
    when(call.execute()).thenAnswer(invocation -> {
      loadingThread.set(Thread.currentThread());
      return Response.success(List.of(rate));
    });
    try (var requests = Executors.newVirtualThreadPerTaskExecutor()) {
      var first = requests.submit(() -> service.queryBulkFundingRates(List.of("BTCUSDT"),
          null, null, 1)).get(3, TimeUnit.SECONDS);
      var next = requests.submit(() -> service.queryBulkFundingRates(List.of(" BTCUSDT "),
          null, null, 1)).get(3, TimeUnit.SECONDS);
      assertSame(first, next);
      assertFalse(loadingThread.get().isVirtual());
      assertTrue(loadingThread.get().getName().startsWith("funding-loader-"));
      assertEquals("0.0001", first.fundingRates().get("BTCUSDT").getFirst().getFundingRate());
      verify(call, times(1)).execute();
    } finally {
      service.closeFundingLoads();
    }
  }
}
