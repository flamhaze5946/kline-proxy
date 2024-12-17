package com.zx.quant.klineproxy.controller;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.service.StatisticService;
import com.zx.quant.klineproxy.util.CommonUtil;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * statistic controller
 * @author flamhaze5946
 */
@RestController
@RequestMapping("statistic")
public class StatisticController {

  private static final String YAMA_ALT_DATE_PATTERN = "yyyy-MM-dd";

  private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = ThreadLocal.withInitial(() ->
      new SimpleDateFormat(YAMA_ALT_DATE_PATTERN)
  );

  private static final String YAMA_01_INDEX_TITLE = "yama 01 index";

  private static final String YAMA_02_INDEX_TITLE = "yama 02 index";

  private static final String YAMA_AGG_INDEX_TITLE = "agg yama index";

  private final LoadingCache<String, byte[]> imageCache = Caffeine.newBuilder()
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .maximumSize(24)
      .build(title -> {
        if (StringUtils.equals(title, YAMA_01_INDEX_TITLE)) {
          return CommonUtil.saveArrayToPng(YAMA_01_INDEX_TITLE, convertToDateMap(getYama01AltCoinIndex()));
        } else if (StringUtils.equals(title, YAMA_02_INDEX_TITLE)) {
          return CommonUtil.saveArrayToPng(YAMA_02_INDEX_TITLE, convertToDateMap(getYama02AltCoinIndex()));
        } else if (StringUtils.equals(title, YAMA_AGG_INDEX_TITLE)) {
          return CommonUtil.saveArrayToPng(YAMA_AGG_INDEX_TITLE, convertToDateMap(getYamaAggAltCoinIndex()));
        } else {
          throw new RuntimeException("invalid title");
        }
      });


  @Autowired
  private StatisticService statisticService;


  @GetMapping("getAltCoinIndex")
  public Map<String, Float> getAltCoinIndex() throws IOException {
    return statisticService.getAltCoinIndex();
  }

  @GetMapping("getYama01AltCoinIndex")
  public Map<Long, Float> getYama01AltCoinIndex() throws IOException {
    return statisticService.getYama01AltCoinIndex().entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getIndex()));
  }

  @GetMapping("pic/getYama01AltCoinIndex")
  public ResponseEntity<byte[]> getYama01AltCoinIndexPic() throws IOException {
    byte[] imageBytes = imageCache.get(YAMA_01_INDEX_TITLE);

    return ResponseEntity
        .ok()
        .contentType(MediaType.IMAGE_PNG)
        .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"image.png\"")
        .body(imageBytes);
  }

  @GetMapping("getYama02AltCoinIndex")
  public Map<Long, Float> getYama02AltCoinIndex() throws IOException {
    return statisticService.getYama02AltCoinIndex().entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getIndex()));
  }

  @GetMapping("pic/getYama02AltCoinIndex")
  public ResponseEntity<byte[]> getYama02AltCoinIndexPic() throws IOException {
    byte[] imageBytes = imageCache.get(YAMA_02_INDEX_TITLE);

    return ResponseEntity
        .ok()
        .contentType(MediaType.IMAGE_PNG)
        .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"image.png\"")
        .body(imageBytes);
  }

  @GetMapping("getYamaAggAltCoinIndex")
  public Map<Long, Float> getYamaAggAltCoinIndex() {
    Map<Long, Float> yama01Index = statisticService.getYama01AltCoinIndex().entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getIndex()));
    Map<Long, Float> yama02Index = statisticService.getYama02AltCoinIndex().entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().getIndex()));
    Map<Long, Float> yamaAggIndex = new TreeMap<>();
    yama01Index.forEach((openTime, indexValue) -> {
      Float mergedIndexValue = yamaAggIndex.getOrDefault(openTime, 0f);
      yamaAggIndex.put(openTime, mergedIndexValue + indexValue);
    });
    yama02Index.forEach((openTime, indexValue) -> {
      Float mergedIndexValue = yamaAggIndex.getOrDefault(openTime, 0f);
      yamaAggIndex.put(openTime, mergedIndexValue + indexValue);
    });
    return yamaAggIndex;
  }

  @GetMapping("pic/getYamaAggAltCoinIndex")
  public ResponseEntity<byte[]> getYamaAggAltCoinIndexPic() throws IOException {
    byte[] imageBytes = imageCache.get(YAMA_AGG_INDEX_TITLE);

    return ResponseEntity
        .ok()
        .contentType(MediaType.IMAGE_PNG)
        .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"image.png\"")
        .body(imageBytes);
  }

  private Map<String, Float> convertToDateMap(Map<Long, Float> originalMap) {
    return originalMap.entrySet().stream()
        .collect(Collectors.toMap(entry -> DATE_FORMATTER.get().format(new Date(entry.getKey())),
            Entry::getValue));
  }
}
