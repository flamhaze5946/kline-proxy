package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.model.Kline;
import com.zx.quant.klineproxy.model.KlineSetKey;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.enums.IntervalEnum;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.util.Serializer;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Production-scale cold-load timing. Not part of the regular suite; run with:
 * ./mvnw test -Dtest=ColdDataLoadBenchmarkTest -Dcoldload.benchmark=true
 */
class ColdDataLoadBenchmarkTest {

  private static final long HOUR = IntervalEnum.ONE_HOUR.getMills();

  private static final int SYMBOLS = 40;

  private static final int ROWS_PER_SYMBOL = 9000;

  private static final long BASE_TIME = LocalDate.parse("2025-07-01")
      .atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();

  @TempDir
  Path tempDir;

  @Test
  @EnabledIfSystemProperty(named = "coldload.benchmark", matches = "true")
  void benchmarkDumpAndColdLoadAtScale() {
    KlinePersistenceProperties properties = new KlinePersistenceProperties();
    properties.setRootDir(tempDir.toString());
    JsonKlinePersistenceStore store = new JsonKlinePersistenceStore(
        properties, new Serializer(new ObjectMapper()));
    long currentTime = BASE_TIME + (ROWS_PER_SYMBOL + 1) * HOUR;

    List<String> symbols = new ArrayList<>();
    for (int i = 0; i < SYMBOLS; i++) {
      symbols.add("SYM" + i + "USDT");
    }
    List<PersistedKlineRow> rows = new ArrayList<>(ROWS_PER_SYMBOL);
    for (int i = 0; i < ROWS_PER_SYMBOL; i++) {
      PersistedKlineRow row = new PersistedKlineRow();
      long openTime = BASE_TIME + i * HOUR;
      row.setOpenTime(openTime);
      row.setOpenPrice("43251.12345678");
      row.setHighPrice("43299.87654321");
      row.setLowPrice("43201.11111111");
      row.setClosePrice("43275.55555555");
      row.setVolume("123456.789");
      row.setCloseTime(openTime + HOUR - 1);
      row.setQuoteVolume("5341234567.89");
      row.setTradeNum(123456);
      row.setActiveBuyVolume("61728.394");
      row.setActiveBuyQuoteVolume("2670617283.94");
      rows.add(row);
    }

    long dumpStart = System.nanoTime();
    for (String symbol : symbols) {
      store.dumpRows("future", "1h", symbol, rows, ROWS_PER_SYMBOL, currentTime);
    }
    long dumpMillis = (System.nanoTime() - dumpStart) / 1_000_000L;

    long redumpStart = System.nanoTime();
    for (String symbol : symbols) {
      store.dumpRows("future", "1h", symbol, rows, ROWS_PER_SYMBOL, currentTime);
    }
    long redumpMillis = (System.nanoTime() - redumpStart) / 1_000_000L;

    long loadStart = System.nanoTime();
    int loadedRows = 0;
    for (String symbol : symbols) {
      loadedRows += store.loadRows("future", "1h", symbol, ROWS_PER_SYMBOL).size();
    }
    long loadMillis = (System.nanoTime() - loadStart) / 1_000_000L;
    assertEquals(SYMBOLS * ROWS_PER_SYMBOL, loadedRows);

    // full service-level restore: JSON load + row->Kline conversion + map insert
    JsonKlinePersistenceStore futureStore = new FutureCodeStore(properties, new Serializer(new ObjectMapper()));
    ColdDataLoadIntegrationTest.HarnessKlineService service =
        new ColdDataLoadIntegrationTest.HarnessKlineService("double", futureStore, ROWS_PER_SYMBOL);
    Set<KlineSetKey> keys = new HashSet<>();
    for (String symbol : symbols) {
      keys.add(new KlineSetKey(symbol, IntervalEnum.ONE_HOUR.code()));
    }
    long restoreStart = System.nanoTime();
    Set<KlineSetKey> restored = service.invokeRestorePersistedKlines(keys);
    long restoreMillis = (System.nanoTime() - restoreStart) / 1_000_000L;
    assertEquals(SYMBOLS, restored.size());

    long totalRows = (long) SYMBOLS * ROWS_PER_SYMBOL;
    // fleet estimate: future 1h ~470x9000 + spot 1h ~400x18000 + 1d ~870x2000 ≈ 13M rows
    double fleetRows = 13_000_000.0;
    System.out.printf("%n=== cold-load benchmark: %d symbols x %d rows (%d rows total) ===%n",
        SYMBOLS, ROWS_PER_SYMBOL, totalRows);
    System.out.printf("initial dump : %6d ms (%.1f us/row)%n", dumpMillis, dumpMillis * 1000.0 / totalRows);
    System.out.printf("steady redump: %6d ms (sealed shards skipped)%n", redumpMillis);
    System.out.printf("store load   : %6d ms (%.1f us/row)%n", loadMillis, loadMillis * 1000.0 / totalRows);
    System.out.printf("full restore : %6d ms (%.1f us/row)%n", restoreMillis, restoreMillis * 1000.0 / totalRows);
    System.out.printf("fleet estimate (~13M rows): restore ≈ %.1f s serial%n",
        restoreMillis / 1000.0 * fleetRows / totalRows);
  }

  /** store that reports service code "future" regardless of the harness's "spot" code. */
  private static class FutureCodeStore extends JsonKlinePersistenceStore {

    FutureCodeStore(KlinePersistenceProperties properties, Serializer serializer) {
      super(properties, serializer);
    }

    @Override
    public List<PersistedKlineRow> loadRows(String service, String interval, String symbol, int maxStoreCount) {
      return super.loadRows("future", interval, symbol, maxStoreCount);
    }

    @Override
    public void dumpRows(String service, String interval, String symbol, List<PersistedKlineRow> rows,
                         int maxStoreCount, long currentTime) {
      super.dumpRows("future", interval, symbol, rows, maxStoreCount, currentTime);
    }
  }
}
