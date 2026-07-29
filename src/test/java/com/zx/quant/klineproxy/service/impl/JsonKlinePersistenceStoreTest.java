package com.zx.quant.klineproxy.service.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineManifest;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.util.Serializer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class JsonKlinePersistenceStoreTest {

  @TempDir
  Path tempDir;

  @Test
  void dumpRowsShouldDeleteExpiredShardFilesAndTrimBoundaryShard() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    List<PersistedKlineRow> rows = List.of(
        buildRow("2026-03-28", 0),
        buildRow("2026-03-28", 1),
        buildRow("2026-03-29", 0),
        buildRow("2026-03-29", 1),
        buildRow("2026-03-30", 0),
        buildRow("2026-03-30", 1)
    );

    store.dumpRows("spot", "1h", "BTCUSDT", rows, 3, dayStart("2026-03-30") + (12L * 3_600_000L));

    Path symbolDir = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT");
    assertFalse(Files.exists(symbolDir.resolve("2026-03-28.json")));
    assertTrue(Files.exists(symbolDir.resolve("2026-03-29.json")));
    assertTrue(Files.exists(symbolDir.resolve("2026-03-30.json")));

    Serializer serializer = new Serializer(new ObjectMapper());
    PersistedKlineManifest manifest = serializer.fromJsonString(
        Files.readString(symbolDir.resolve("_meta.json"), StandardCharsets.UTF_8), PersistedKlineManifest.class);
    assertEquals(3, manifest.getStoredCount());
    assertEquals("2026-03-29", manifest.getBoundaryDay());

    List<PersistedKlineRow> loadedRows = store.loadRows("spot", "1h", "BTCUSDT", 3);
    assertEquals(3, loadedRows.size());
    assertEquals(rows.get(3).getOpenTime(), loadedRows.get(0).getOpenTime());
    assertEquals(rows.get(5).getOpenTime(), loadedRows.get(2).getOpenTime());
  }

  @Test
  void dumpRowsShouldNotRewriteSealedShardFiles() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    List<PersistedKlineRow> rows = List.of(
        buildRow("2026-03-28", 0),
        buildRow("2026-03-29", 0),
        buildRow("2026-03-30", 0)
    );
    long currentTime = dayStart("2026-03-30") + (12L * 3_600_000L);
    store.dumpRows("spot", "1h", "BTCUSDT", rows, 3, currentTime);

    Path sealedShard = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT").resolve("2026-03-29.json");
    long firstModifiedTime = Files.getLastModifiedTime(sealedShard).toMillis();
    Thread.sleep(5L);

    store.dumpRows("spot", "1h", "BTCUSDT", rows, 3, currentTime);

    long secondModifiedTime = Files.getLastModifiedTime(sealedShard).toMillis();
    assertEquals(firstModifiedTime, secondModifiedTime);
  }

  @Test
  void dumpRowsShouldRewriteSealedShardWhenRowCountChanges() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    long currentTime = dayStart("2026-03-30") + (12L * 3_600_000L);
    // day 2026-03-29 sealed with a hole (only hour 0)
    store.dumpRows("spot", "1h", "BTCUSDT", List.of(
        buildRow("2026-03-28", 0),
        buildRow("2026-03-29", 0),
        buildRow("2026-03-30", 0)
    ), 10, currentTime);
    Path sealedShard = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT").resolve("2026-03-29.json");
    Serializer serializer = new Serializer(new ObjectMapper());

    // warmup backfilled hour 1 of the sealed day -> shard must be rewritten
    store.dumpRows("spot", "1h", "BTCUSDT", List.of(
        buildRow("2026-03-28", 0),
        buildRow("2026-03-29", 0),
        buildRow("2026-03-29", 1),
        buildRow("2026-03-30", 0)
    ), 10, currentTime);

    var shard = serializer.fromJsonString(
        Files.readString(sealedShard, StandardCharsets.UTF_8),
        com.zx.quant.klineproxy.model.persistence.PersistedKlineShardFile.class);
    assertEquals(2, shard.getRows().size());
  }

  @Test
  void dumpRowsShouldRewriteSealedShardWhenTradeNumChangesWithSameCount() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    long currentTime = dayStart("2026-03-30") + (12L * 3_600_000L);
    PersistedKlineRow placeholder = buildRow("2026-03-29", 0);
    placeholder.setTradeNum(0);
    store.dumpRows("spot", "1h", "BTCUSDT", List.of(
        buildRow("2026-03-28", 0),
        placeholder,
        buildRow("2026-03-30", 0)
    ), 10, currentTime);
    Path sealedShard = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT").resolve("2026-03-29.json");
    Serializer serializer = new Serializer(new ObjectMapper());

    // same row count, but the filled placeholder got replaced by the real kline
    PersistedKlineRow realRow = buildRow("2026-03-29", 0);
    realRow.setTradeNum(42);
    store.dumpRows("spot", "1h", "BTCUSDT", List.of(
        buildRow("2026-03-28", 0),
        realRow,
        buildRow("2026-03-30", 0)
    ), 10, currentTime);

    var shard = serializer.fromJsonString(
        Files.readString(sealedShard, StandardCharsets.UTF_8),
        com.zx.quant.klineproxy.model.persistence.PersistedKlineShardFile.class);
    assertEquals(42, shard.getRows().get(0).getTradeNum());
  }

  @Test
  void loadRowsShouldSweepLeftoverTempFilesFromCrashedWrites() throws Exception {
    JsonKlinePersistenceStore store = buildStore();
    long currentTime = dayStart("2026-03-30") + (12L * 3_600_000L);
    store.dumpRows("spot", "1h", "BTCUSDT", List.of(buildRow("2026-03-30", 0)), 5, currentTime);
    Path symbolDir = tempDir.resolve("spot").resolve("1h").resolve("BTCUSDT");
    Path crashTemp = symbolDir.resolve("2026-03-30.json.123-456.tmp");
    Path manifestTemp = symbolDir.resolve("_meta.json.789-012.tmp");
    Files.writeString(crashTemp, "partial", StandardCharsets.UTF_8);
    Files.writeString(manifestTemp, "partial", StandardCharsets.UTF_8);

    List<PersistedKlineRow> loadedRows = store.loadRows("spot", "1h", "BTCUSDT", 5);

    assertEquals(1, loadedRows.size());
    assertFalse(Files.exists(crashTemp));
    assertFalse(Files.exists(manifestTemp));
  }

  private JsonKlinePersistenceStore buildStore() {
    KlinePersistenceProperties properties = new KlinePersistenceProperties();
    properties.setRootDir(tempDir.toString());
    return new JsonKlinePersistenceStore(properties, new Serializer(new ObjectMapper()));
  }

  private PersistedKlineRow buildRow(String day, int hour) {
    long openTime = dayStart(day) + (hour * 3_600_000L);
    PersistedKlineRow row = new PersistedKlineRow();
    row.setOpenTime(openTime);
    row.setOpenPrice("1");
    row.setHighPrice("1");
    row.setLowPrice("1");
    row.setClosePrice("1");
    row.setVolume("1");
    row.setCloseTime(openTime + 3_600_000L - 1);
    row.setQuoteVolume("1");
    row.setTradeNum(1);
    row.setActiveBuyVolume("1");
    row.setActiveBuyQuoteVolume("1");
    return row;
  }

  private long dayStart(String day) {
    return LocalDate.parse(day).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
  }
}
