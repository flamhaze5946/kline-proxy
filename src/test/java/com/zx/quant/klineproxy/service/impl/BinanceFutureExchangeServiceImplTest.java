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

  private static final long HOUR_MS = 60L * 60L * 1000L;

  private static BinanceFutureExchangeServiceImpl serviceWith(long graceMs) {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    ReflectionTestUtils.setField(service, "binanceFutureClient", mock(BinanceFutureClient.class));
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));
    ReflectionTestUtils.setField(service, "fundingPublicationGraceMs", graceMs);
    return service;
  }

  /**
   * The fix this change exists for. The Rust trader's Lambda children align to
   * the hour boundary and reach this service ~25-40ms after it — inside the
   * publication grace, before Binance has finished publishing the tick that
   * settled at that boundary. They must be parked, not answered with a
   * premature (and therefore empty) fetch that the trader would read as fr=0
   * while the backtest sees the real rate.
   */
  @Test
  void aCallerInsideThePublicationGraceMustBeParkedNotAnsweredEarly() {
    long graceMs = 200L;
    BinanceFutureExchangeServiceImpl service = serviceWith(graceMs);

    long boundaryStartingNow = System.currentTimeMillis();
    long before = System.nanoTime();
    ReflectionTestUtils.invokeMethod(service, "awaitPublicationGrace", boundaryStartingNow);
    long parkedMs = (System.nanoTime() - before) / 1_000_000L;

    assertTrue(parkedMs >= graceMs - 20L,
        "must park until publication, parked=" + parkedMs + "ms");
    assertTrue(parkedMs <= graceMs + 500L,
        "the park is bounded by the grace itself, parked=" + parkedMs + "ms");
  }

  /**
   * A settled boundary costs nothing. This is what keeps the wait off the
   * order-placement path on the 21 hours a day that never publish funding: the
   * condition is wall-clock, not "wait until non-empty".
   */
  @Test
  void anAlreadySettledBoundaryMustNotBeParked() {
    BinanceFutureExchangeServiceImpl service = serviceWith(200L);

    long before = System.nanoTime();
    ReflectionTestUtils.invokeMethod(service, "awaitPublicationGrace",
        System.currentTimeMillis() - HOUR_MS);
    long parkedMs = (System.nanoTime() - before) / 1_000_000L;

    assertTrue(parkedMs < 50L, "historical boundary must return immediately, parked=" + parkedMs);
  }

  /**
   * Binance returns the EARLIEST `limit` rows when a time range brackets more
   * than that ("response will be sent as startTime + limit"), while our own trim
   * keeps the NEWEST. Forwarding the caller's limit would therefore drop the
   * most recent event — exactly the one the caller's latest candle needs — and
   * no local trim could recover a row that never arrived.
   */
  @Test
  void aRangedPerSymbolCallMustRequestAFullUpstreamPage() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(eq("BTCUSDT"), any(Long.class), any(Long.class), eq(1000)))
        .willReturn(call);
    given(call.execute()).willReturn(Response.success(List.of()));

    // Wider than FUNDING_CHUNK_CACHE_WINDOW_THRESHOLD_MS, so it takes the
    // per-symbol path where the limit is forwarded upstream.
    service.queryBulkFundingRates(List.of("BTCUSDT"), 0L, 9 * HOUR_MS, 4);

    verify(client, times(1)).getFundingRates(eq("BTCUSDT"), any(Long.class), any(Long.class), eq(1000));
    verify(client, never()).getFundingRates(eq("BTCUSDT"), any(), any(), eq(4));
  }

  /**
   * A one-sided range is still a bounded window as far as the direct loader is
   * concerned, so it must get the ranged result ceiling rather than the
   * no-range cap meant to stop unbounded responses.
   */
  @Test
  void aOneSidedRangeMustGetTheRangedResultCeiling() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    List<FutureFundingRate> rows = new java.util.ArrayList<>();
    for (int i = 0; i < 300; i++) {
      FutureFundingRate row = new FutureFundingRate();
      row.setSymbol("BTCUSDT");
      row.setFundingTime(1_000L + i);
      row.setFundingRate(new BigDecimal("0.0001"));
      row.setMarkPrice(new BigDecimal("100"));
      rows.add(row);
    }
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(eq("BTCUSDT"), eq(0L), isNull(), eq(1000))).willReturn(call);
    given(call.execute()).willReturn(Response.success(rows));

    var resp = service.queryBulkFundingRates(List.of("BTCUSDT"), 0L, null, 300);

    assertEquals(300, resp.fundingRates().get("BTCUSDT").size(),
        "a since-only caller asking for 300 must get 300, not the no-range cap of 100");
  }

  /**
   * A degenerate window must be answered centrally. Routed to the per-symbol
   * loader it would cost one Binance call per symbol — ~526 of them — to
   * discover that an empty interval is empty.
   */
  @Test
  void aDegenerateRangeMustNotReachAnyLoader() {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    var resp = service.queryBulkFundingRates(List.of("BTCUSDT", "ETHUSDT"), HOUR_MS, HOUR_MS, 5);

    assertTrue(resp.fundingRates().isEmpty(), "an empty interval yields an empty response");
    verify(client, never()).getFundingRates(any(), any(), any(), any());
  }

  /**
   * Drives the real entry point rather than the helper: concurrent callers for
   * the CURRENT boundary must each observe the publication grace and must
   * collapse onto ONE upstream fetch, so every child of a trading cycle reads
   * the same funding snapshot. The two timing tests above still pass if
   * `loadChunk` stopped calling `awaitPublicationGrace`; this one does not.
   */
  @Test
  void concurrentCurrentBoundaryCallersShareOneGracedFetch() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    // The boundary under test is the CURRENT hour's, which started some minutes
    // ago. Size the grace so that boundary is still inside its publication
    // window right now — that is the state a Lambda child arrives in at
    // HH:00:00.03, reproduced deterministically without waiting for an hour tick.
    long boundary = Math.floorDiv(System.currentTimeMillis(), HOUR_MS) * HOUR_MS;
    long parkMs = 400L;

    java.util.concurrent.atomic.AtomicLong boundaryFetchAtMs =
        new java.util.concurrent.atomic.AtomicLong(Long.MAX_VALUE);
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(isNull(), any(Long.class), any(Long.class), eq(1000)))
        .willAnswer(invocation -> {
          if (boundary == (Long) invocation.getArgument(1)) {
            boundaryFetchAtMs.updateAndGet(prev -> Math.min(prev, System.currentTimeMillis()));
          }
          return call;
        });
    given(call.execute()).willReturn(Response.success(List.of()));

    long sinceMs = boundary - HOUR_MS;
    long untilMs = boundary + 1L;

    int callers = 4;
    java.util.concurrent.CountDownLatch start = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.ExecutorService pool =
        java.util.concurrent.Executors.newFixedThreadPool(callers);
    List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
    for (int i = 0; i < callers; i++) {
      futures.add(pool.submit(() -> {
        start.await();
        service.queryBulkFundingRates(List.of("BTCUSDT"), sinceMs, untilMs, 2);
        return null;
      }));
    }
    // Set the grace HERE, immediately before releasing workers that are already
    // parked on the latch. Establishing it before mock and pool setup would let
    // setup eat the park, so a build slow enough could pass with the park
    // removed entirely.
    long graceMs = (System.currentTimeMillis() - boundary) + parkMs;
    ReflectionTestUtils.setField(service, "fundingPublicationGraceMs", graceMs);
    try {
      start.countDown();
      for (java.util.concurrent.Future<?> f : futures) {
        f.get(30, java.util.concurrent.TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    // Assert against the ABSOLUTE grace expiry, not a later wall-clock reading:
    // anchoring on the latch release would fail spuriously whenever setup took
    // longer than the slack, and would pass a removed park if post-release
    // scheduling happened to be slow.
    assertTrue(boundaryFetchAtMs.get() >= boundary + graceMs,
        "the current boundary must not be fetched before its publication grace elapses;"
            + " fetched " + (boundaryFetchAtMs.get() - (boundary + graceMs))
            + "ms relative to grace expiry");
    verify(client, times(1))
        .getFundingRates(isNull(), eq(boundary), eq(boundary + HOUR_MS), eq(1000));
  }

  /**
   * The newest-N trim must actually run on a response bigger than the caller's
   * limit — the previous paging test only pinned the upstream argument.
   */
  @Test
  void anOverLimitResponseMustBeTrimmedToTheNewestEvents() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    List<FutureFundingRate> rows = new java.util.ArrayList<>();
    for (int i = 0; i < 10; i++) {
      FutureFundingRate row = new FutureFundingRate();
      row.setSymbol("BTCUSDT");
      row.setFundingTime(1_000L + i);
      row.setFundingRate(new BigDecimal("0.000" + i));
      row.setMarkPrice(new BigDecimal("100"));
      rows.add(row);
    }
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(eq("BTCUSDT"), eq(0L), isNull(), eq(1000))).willReturn(call);
    given(call.execute()).willReturn(Response.success(rows));

    var resp = service.queryBulkFundingRates(List.of("BTCUSDT"), 0L, null, 3);
    List<com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate> kept = resp.fundingRates().get("BTCUSDT");

    assertEquals(3, kept.size(), "must keep exactly the requested count");
    assertEquals(1_007L, kept.get(0).getFundingTime(), "and they must be the NEWEST three");
    assertEquals(1_009L, kept.get(2).getFundingTime());
  }

  /** Inverted, not merely empty, ranges must also short-circuit. */
  @Test
  void anInvertedRangeMustNotReachAnyLoader() {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    var resp = service.queryBulkFundingRates(List.of("BTCUSDT"), 2 * HOUR_MS, HOUR_MS, 5);

    assertTrue(resp.fundingRates().isEmpty(), "since > until yields an empty response");
    verify(client, never()).getFundingRates(any(), any(), any(), any());
  }

  /**
   * A bounded range whose page comes back full may span more than one page, and
   * Binance truncates such a range at the NEWEST end — precisely the events a
   * caller wants. Returning it would be a wrong answer that reports success, so
   * the request fails and `nos-runtime` takes its direct-Binance fallback.
   */
  @Test
  void aSaturatedBoundedPageMustFailRatherThanReturnTruncatedRows() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    List<FutureFundingRate> full = new java.util.ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      FutureFundingRate row = new FutureFundingRate();
      row.setSymbol("BTCUSDT");
      row.setFundingTime(1_000L + i);
      row.setFundingRate(new BigDecimal("0.0001"));
      row.setMarkPrice(new BigDecimal("100"));
      full.add(row);
    }
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(eq("BTCUSDT"), eq(0L), any(Long.class), eq(1000)))
        .willReturn(call);
    given(call.execute()).willReturn(Response.success(full));

    // 2000 hours of room for at most 1000 rows: this range really can hold more
    // than one page, so a full page proves nothing arrived from the newest end.
    com.zx.quant.klineproxy.model.exceptions.ApiException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            com.zx.quant.klineproxy.model.exceptions.ApiException.class,
            () -> service.queryBulkFundingRates(List.of("BTCUSDT"), 0L, 2000 * HOUR_MS, 4),
            "a saturated bounded page must not be returned as if it were complete");
    assertEquals(org.springframework.http.HttpStatus.BAD_REQUEST, thrown.getStatus(),
        "must be terminal: GlobalExceptionConfig flattens anything else to 500, and the"
            + " Rust client retries 5xx three times before failing the fanout");
  }

  /**
   * The counterpart, and the one that would have caught the wrong
   * "Lambda-unreachable" claim: a two-sided range with room for at most one
   * page is COMPLETE however full it comes back, and must not be rejected.
   * `MAX_PROXY_WINDOW_HOURS` is 999 precisely so the trader's widest possible
   * request lands here rather than in the branch above.
   */
  @Test
  void aFullPageIsNotRejectedWhenTheRangeCannotHoldMoreThanOne() throws Exception {
    BinanceFutureExchangeServiceImpl service = new BinanceFutureExchangeServiceImpl();
    BinanceFutureClient client = mock(BinanceFutureClient.class);
    ReflectionTestUtils.setField(service, "binanceFutureClient", client);
    ReflectionTestUtils.setField(service, "rateLimitManager", mock(RateLimitManager.class));

    // Exactly the shape `kline_proxy_window(target, 999)` produces.
    long target = 10_000L * HOUR_MS;
    long sinceMs = target - 998 * HOUR_MS - 60_000L;
    long untilMs = target + 60_000L + 1L;

    List<FutureFundingRate> full = new java.util.ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      FutureFundingRate row = new FutureFundingRate();
      row.setSymbol("BTCUSDT");
      row.setFundingTime(sinceMs + 60_000L + (long) i * HOUR_MS);
      row.setFundingRate(new BigDecimal("0.0001"));
      row.setMarkPrice(new BigDecimal("100"));
      full.add(row);
    }
    @SuppressWarnings("unchecked")
    Call<List<FutureFundingRate>> call = (Call<List<FutureFundingRate>>) mock(Call.class);
    given(client.getFundingRates(eq("BTCUSDT"), eq(sinceMs), eq(untilMs), eq(1000)))
        .willReturn(call);
    given(call.execute()).willReturn(Response.success(full));

    var resp = service.queryBulkFundingRates(List.of("BTCUSDT"), sinceMs, untilMs, 999);

    assertEquals(999, resp.fundingRates().get("BTCUSDT").size(),
        "the widest window the trader can generate must be served, not rejected");
  }
}
