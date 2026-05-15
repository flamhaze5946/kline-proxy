package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.model.FutureFundingRate;
import com.zx.quant.klineproxy.util.ConvertUtil;
import com.zx.quant.klineproxy.util.ConvertUtil.DisplayFundingRate;
import com.zx.quant.klineproxy.util.ExceptionSafeRunnable;
import com.zx.quant.klineproxy.util.ThreadFactoryUtil;
import jakarta.annotation.PreDestroy;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Pre-fills {@link BinanceFutureExchangeServiceImpl}'s historical funding
 * chunk cache from Binance Vision monthly archives at startup. Vision
 * only publishes monthly funding-rate archives (no daily), and the latest
 * month is published a few days into the following month — so this
 * warm-up covers historical days for which Vision data is available, and
 * leaves the most-recent days (and the current month) to the existing
 * lazy chunk loader.
 *
 * @author flamhaze5946
 */
@Slf4j
@Component
public class BinanceVisionFundingHistoryLoader {

  private static final String VISION_URL_TEMPLATE =
      "https://data.binance.vision/data/futures/um/monthly/fundingRate/%s/%s-fundingRate-%s.zip";

  private static final String LOADER_THREAD_GROUP = "visionFundingLoader";

  private static final int LOOKBACK_DAYS = 30;

  private static final int DOWNLOAD_CONCURRENCY = 16;

  private static final long HOUR_MS = 60L * 60L * 1000L;

  private static final long DAY_MS = 24L * HOUR_MS;

  private static final DateTimeFormatter MONTH_FMT = DateTimeFormatter.ofPattern("yyyy-MM");

  // Exact header the Vision archives have published consistently. We
  // require an exact match (not just startsWith) so a column reorder
  // such as `calc_time,last_funding_rate,funding_interval_hours` would
  // fail the month rather than silently parse zero numeric values out
  // of the wrong columns.
  private static final String EXPECTED_CSV_HEADER = "calc_time,funding_interval_hours,last_funding_rate";

  // Wrap the shared named-thread factory to produce daemon threads, so a
  // long-running warm-up cannot block JVM shutdown (the warm task uses an
  // uninterruptible CompletableFuture.join, so non-daemon threads would
  // hold the JVM alive past @PreDestroy.shutdownNow()).
  private final ExecutorService pool = Executors.newFixedThreadPool(
      DOWNLOAD_CONCURRENCY, daemonNamedThreadFactory(LOADER_THREAD_GROUP));

  private static ThreadFactory daemonNamedThreadFactory(String groupName) {
    ThreadFactory base = ThreadFactoryUtil.getNamedThreadFactory(groupName);
    return runnable -> {
      Thread t = base.newThread(runnable);
      t.setDaemon(true);
      return t;
    };
  }

  private final OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectTimeout(10, TimeUnit.SECONDS)
      .readTimeout(30, TimeUnit.SECONDS)
      .build();

  @Autowired
  private BinanceFutureExchangeServiceImpl exchangeService;

  @EventListener(ApplicationReadyEvent.class)
  public void onApplicationReady() {
    pool.submit(new ExceptionSafeRunnable(this::warm));
  }

  @PreDestroy
  public void shutdown() {
    pool.shutdownNow();
  }

  void warm() {
    List<String> symbols = exchangeService.querySymbols();
    if (symbols.isEmpty()) {
      log.warn("Vision funding warm-up skipped: no active symbols");
      return;
    }

    long t0 = System.currentTimeMillis();
    long windowEnd = t0;
    long windowStart = windowEnd - LOOKBACK_DAYS * DAY_MS;

    LocalDate startDate = Instant.ofEpochMilli(windowStart).atZone(ZoneOffset.UTC).toLocalDate();
    LocalDate endDate = Instant.ofEpochMilli(windowEnd).atZone(ZoneOffset.UTC).toLocalDate();
    YearMonth startYM = YearMonth.from(startDate);
    YearMonth endYM = YearMonth.from(endDate);

    List<YearMonth> months = new ArrayList<>();
    for (YearMonth ym = startYM; !ym.isAfter(endYM); ym = ym.plusMonths(1)) {
      months.add(ym);
    }

    log.info("Vision funding warm-up starting: symbols={}, months={}, window=[{}, {}]",
        symbols.size(), months.size(), startDate, endDate);

    ConcurrentMap<Long, ConcurrentMap<String, List<DisplayFundingRate>>> staged = new ConcurrentHashMap<>();
    // Months where at least one (symbol, ym) download or parse failed
    // for reasons OTHER than 404 (404 = archive legitimately missing for
    // that symbol). We skip seeding any chunk in a failed month so the
    // lazy chunk loader can fetch the full set from Binance API.
    Set<YearMonth> failedMonths = ConcurrentHashMap.newKeySet();

    List<CompletableFuture<Void>> tasks = new ArrayList<>(symbols.size() * months.size());
    for (String symbol : symbols) {
      for (YearMonth ym : months) {
        tasks.add(CompletableFuture.runAsync(
            () -> downloadAndStage(symbol, ym, windowStart, windowEnd, staged, failedMonths), pool));
      }
    }
    CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();

    int chunkCount = 0;
    int skippedChunks = 0;
    int totalRows = 0;
    for (Map.Entry<Long, ConcurrentMap<String, List<DisplayFundingRate>>> entry : staged.entrySet()) {
      long chunkStart = entry.getKey();
      YearMonth chunkYm = YearMonth.from(
          Instant.ofEpochMilli(chunkStart).atZone(ZoneOffset.UTC).toLocalDate());
      if (failedMonths.contains(chunkYm)) {
        skippedChunks++;
        continue;
      }
      Map<String, List<DisplayFundingRate>> chunk = new LinkedHashMap<>();
      for (Map.Entry<String, List<DisplayFundingRate>> bucket : entry.getValue().entrySet()) {
        List<DisplayFundingRate> sorted = new ArrayList<>(bucket.getValue());
        sorted.sort(Comparator.comparing(DisplayFundingRate::getFundingTime));
        chunk.put(bucket.getKey(), sorted);
        totalRows += sorted.size();
      }
      exchangeService.seedHistoricalChunk(chunkStart, chunk);
      chunkCount++;
    }

    log.info("Vision funding warm-up done: chunksSeeded={}, chunksSkipped={}, failedMonths={}, totalRows={}, elapsedMs={}",
        chunkCount, skippedChunks, failedMonths, totalRows, System.currentTimeMillis() - t0);
  }

  private void downloadAndStage(String symbol, YearMonth ym, long windowStart, long windowEnd,
      ConcurrentMap<Long, ConcurrentMap<String, List<DisplayFundingRate>>> staged,
      Set<YearMonth> failedMonths) {
    String url = String.format(VISION_URL_TEMPLATE, symbol, symbol, MONTH_FMT.format(ym));
    Request request = new Request.Builder().url(url).get().build();
    try (Response resp = httpClient.newCall(request).execute()) {
      if (resp.code() == 404) {
        // Missing archive: month not yet published, or symbol had no
        // funding in that month. Not a failure for our purposes.
        return;
      }
      if (!resp.isSuccessful()) {
        log.debug("Vision fetch {} returned {}", url, resp.code());
        failedMonths.add(ym);
        return;
      }
      ResponseBody body = resp.body();
      if (body == null) {
        failedMonths.add(ym);
        return;
      }
      parseAndStage(symbol, body.bytes(), windowStart, windowEnd, staged);
    } catch (Exception ex) {
      log.debug("Vision fetch/parse failed for {} {}: {}", symbol, ym, ex.toString());
      failedMonths.add(ym);
    }
  }

  private void parseAndStage(String symbol, byte[] zipBytes, long windowStart, long windowEnd,
      ConcurrentMap<Long, ConcurrentMap<String, List<DisplayFundingRate>>> staged) throws Exception {
    try (ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(zipBytes))) {
      ZipEntry entry = zin.getNextEntry();
      if (entry == null) {
        throw new IllegalStateException("Vision archive empty for " + symbol);
      }
      BufferedReader reader = new BufferedReader(new InputStreamReader(zin, StandardCharsets.UTF_8));
      String header = reader.readLine();
      if (header == null || !EXPECTED_CSV_HEADER.equals(header.trim())) {
        // Strict match: column reorder, drift, BOM, or garbled content
        // all fail the month so lazy-load takes over for those chunks.
        throw new IllegalStateException(
            "Unexpected Vision header for " + symbol + ": " + header);
      }
      String line;
      while ((line = reader.readLine()) != null) {
        String[] parts = line.split(",");
        if (parts.length < 3) {
          continue;
        }
        long fundingTime;
        BigDecimal rate;
        try {
          fundingTime = Long.parseLong(parts[0].trim());
          rate = new BigDecimal(parts[2].trim());
        } catch (NumberFormatException nfe) {
          continue;
        }
        if (fundingTime < windowStart || fundingTime >= windowEnd) {
          continue;
        }
        FutureFundingRate row = new FutureFundingRate();
        row.setSymbol(symbol);
        row.setFundingTime(fundingTime);
        row.setFundingRate(rate);
        DisplayFundingRate display = ConvertUtil.convertToDisplayFundingRate(row);
        long chunkStart = Math.floorDiv(fundingTime, HOUR_MS) * HOUR_MS;
        // synchronizedList guards against the hypothetical case where
        // Vision duplicates a boundary funding event across adjacent
        // monthly archives, which would briefly produce two writers per
        // (chunkStart, symbol) slot when both month tasks run in parallel.
        staged.computeIfAbsent(chunkStart, k -> new ConcurrentHashMap<>())
            .computeIfAbsent(symbol, k -> Collections.synchronizedList(new ArrayList<>()))
            .add(display);
      }
    }
  }
}
