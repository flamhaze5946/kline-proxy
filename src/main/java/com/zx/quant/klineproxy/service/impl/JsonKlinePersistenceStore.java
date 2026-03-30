package com.zx.quant.klineproxy.service.impl;

import com.zx.quant.klineproxy.model.config.KlinePersistenceProperties;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineManifest;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineRow;
import com.zx.quant.klineproxy.model.persistence.PersistedKlineShardFile;
import com.zx.quant.klineproxy.service.KlinePersistenceStore;
import com.zx.quant.klineproxy.util.Serializer;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Service;

/**
 * json kline persistence store
 * @author flamhaze5946
 */
@Slf4j
@Service
@EnableConfigurationProperties(KlinePersistenceProperties.class)
public class JsonKlinePersistenceStore implements KlinePersistenceStore {

  private static final String MANIFEST_FILE_NAME = "_meta.json";

  private final KlinePersistenceProperties persistenceProperties;

  private final Serializer serializer;

  public JsonKlinePersistenceStore(KlinePersistenceProperties persistenceProperties, Serializer serializer) {
    this.persistenceProperties = persistenceProperties;
    this.serializer = serializer;
  }

  @Override
  public List<PersistedKlineRow> loadRows(String service, String interval, String symbol, int maxStoreCount) {
    Path symbolDir = getSymbolDir(service, interval, symbol);
    if (!Files.isDirectory(symbolDir) || maxStoreCount <= 0) {
      return List.of();
    }
    List<Path> shardFiles = listShardFiles(symbolDir);
    if (CollectionUtils.isEmpty(shardFiles)) {
      return List.of();
    }
    Collections.reverse(shardFiles);
    List<PersistedKlineRow> rows = new ArrayList<>(maxStoreCount);
    for (Path shardFile : shardFiles) {
      PersistedKlineShardFile shard = readJson(shardFile, PersistedKlineShardFile.class);
      if (shard == null || CollectionUtils.isEmpty(shard.getRows())) {
        continue;
      }
      List<PersistedKlineRow> shardRows = shard.getRows().stream()
          .filter(Objects::nonNull)
          .sorted(Comparator.comparingLong(PersistedKlineRow::getOpenTime).reversed())
          .toList();
      for (PersistedKlineRow row : shardRows) {
        rows.add(row);
        if (rows.size() >= maxStoreCount) {
          break;
        }
      }
      if (rows.size() >= maxStoreCount) {
        break;
      }
    }
    Collections.reverse(rows);
    return deduplicateAndTail(rows, maxStoreCount);
  }

  @Override
  public void dumpRows(String service, String interval, String symbol, List<PersistedKlineRow> rows,
                       int maxStoreCount, long currentTime) {
    Path symbolDir = getSymbolDir(service, interval, symbol);
    try {
      Files.createDirectories(symbolDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    List<PersistedKlineRow> retainedRows = deduplicateAndTail(rows, maxStoreCount);
    if (CollectionUtils.isEmpty(retainedRows)) {
      deleteAllShardFiles(symbolDir);
      deleteIfExists(symbolDir.resolve(MANIFEST_FILE_NAME));
      cleanupEmptyDirectories(symbolDir);
      return;
    }

    Map<String, List<PersistedKlineRow>> rowsByDay = retainedRows.stream()
        .collect(Collectors.groupingBy(row -> dayString(row.getOpenTime()), LinkedHashMap::new, Collectors.toList()));
    String boundaryDay = rowsByDay.keySet().iterator().next();
    String activeDay = dayString(currentTime);
    List<Path> existingShardFiles = listShardFiles(symbolDir);
    for (Path shardFile : existingShardFiles) {
      String fileName = shardFile.getFileName().toString();
      String day = fileName.substring(0, fileName.length() - 5);
      if (!rowsByDay.containsKey(day)) {
        deleteIfExists(shardFile);
      }
    }

    rowsByDay.forEach((day, dayRows) -> {
      Path shardPath = symbolDir.resolve(day + ".json");
      boolean shouldRewrite = Objects.equals(day, boundaryDay)
          || Objects.equals(day, activeDay)
          || !Files.exists(shardPath);
      if (!shouldRewrite) {
        return;
      }
      PersistedKlineShardFile shardFile = new PersistedKlineShardFile();
      shardFile.setService(service);
      shardFile.setInterval(interval);
      shardFile.setSymbol(symbol);
      shardFile.setDay(day);
      shardFile.setRows(dayRows);
      writeJsonAtomically(shardPath, shardFile);
    });

    PersistedKlineManifest manifest = new PersistedKlineManifest();
    manifest.setService(service);
    manifest.setInterval(interval);
    manifest.setSymbol(symbol);
    manifest.setMaxStoreCount(maxStoreCount);
    manifest.setStoredCount(retainedRows.size());
    manifest.setBoundaryDay(boundaryDay);
    manifest.setActiveDay(activeDay);
    PersistedKlineRow lastRow = retainedRows.get(retainedRows.size() - 1);
    manifest.setLastDumpedCleanOpenTime(lastRow.getOpenTime());
    manifest.setLastDumpedCleanCloseTime(lastRow.getCloseTime());
    writeJsonAtomically(symbolDir.resolve(MANIFEST_FILE_NAME), manifest);
    cleanupEmptyDirectories(symbolDir);
  }

  private List<PersistedKlineRow> deduplicateAndTail(Collection<PersistedKlineRow> rows, int maxStoreCount) {
    if (CollectionUtils.isEmpty(rows) || maxStoreCount <= 0) {
      return List.of();
    }
    Map<Long, PersistedKlineRow> rowMap = rows.stream()
        .filter(Objects::nonNull)
        .sorted(Comparator.comparingLong(PersistedKlineRow::getOpenTime))
        .collect(Collectors.toMap(PersistedKlineRow::getOpenTime, row -> row, (left, right) ->
            left.getTradeNum() >= right.getTradeNum() ? left : right, LinkedHashMap::new));
    List<PersistedKlineRow> deduplicatedRows = new ArrayList<>(rowMap.values());
    if (deduplicatedRows.size() <= maxStoreCount) {
      return deduplicatedRows;
    }
    return List.copyOf(deduplicatedRows.subList(deduplicatedRows.size() - maxStoreCount, deduplicatedRows.size()));
  }

  private List<Path> listShardFiles(Path symbolDir) {
    if (!Files.isDirectory(symbolDir)) {
      return List.of();
    }
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(symbolDir, "*.json")) {
      List<Path> shardFiles = new ArrayList<>();
      for (Path path : stream) {
        if (!String.valueOf(path.getFileName()).equals(MANIFEST_FILE_NAME)) {
          shardFiles.add(path);
        }
      }
      shardFiles.sort(Comparator.comparing(path -> String.valueOf(path.getFileName())));
      return shardFiles;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void deleteAllShardFiles(Path symbolDir) {
    for (Path shardFile : listShardFiles(symbolDir)) {
      deleteIfExists(shardFile);
    }
  }

  private <T> T readJson(Path filePath, Class<T> clazz) {
    try {
      if (!Files.exists(filePath)) {
        return null;
      }
      return serializer.fromJsonString(Files.readString(filePath, StandardCharsets.UTF_8), clazz);
    } catch (Exception e) {
      log.warn("failed to read persisted kline file: {}", filePath, e);
      return null;
    }
  }

  private void writeJsonAtomically(Path targetPath, Object payload) {
    Path tempPath = targetPath.resolveSibling(targetPath.getFileName() + ".tmp");
    try {
      Files.createDirectories(targetPath.getParent());
      Files.writeString(tempPath, serializer.toJsonString(payload), StandardCharsets.UTF_8);
      try {
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tempPath, targetPath, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      deleteIfExists(tempPath);
    }
  }

  private void cleanupEmptyDirectories(Path symbolDir) {
    try {
      if (Files.isDirectory(symbolDir) && isDirectoryEmpty(symbolDir)) {
        Files.deleteIfExists(symbolDir);
      }
      Path intervalDir = symbolDir.getParent();
      if (intervalDir != null && Files.isDirectory(intervalDir) && isDirectoryEmpty(intervalDir)) {
        Files.deleteIfExists(intervalDir);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean isDirectoryEmpty(Path dir) throws IOException {
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
      return !stream.iterator().hasNext();
    }
  }

  private void deleteIfExists(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Path getSymbolDir(String service, String interval, String symbol) {
    return Path.of(persistenceProperties.getRootDir(), service, interval, symbol);
  }

  private String dayString(long openTime) {
    LocalDate day = Instant.ofEpochMilli(openTime).atOffset(ZoneOffset.UTC).toLocalDate();
    return day.toString();
  }
}
