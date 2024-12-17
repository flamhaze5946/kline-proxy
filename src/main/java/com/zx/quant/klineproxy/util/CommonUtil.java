package com.zx.quant.klineproxy.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.StringUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.data.category.DefaultCategoryDataset;

/**
 * common util
 * @author flamhaze5946
 */
public final class CommonUtil {

  private static final Character ARRAY_MESSAGE_PREFIX = '[';


  private static final Map<Class<? extends BaseEnum>, Map<String, BaseEnum>> ENUM_MAP = new ConcurrentHashMap<>();

  public static final int BUFFER = 1024;

  public static void sleep(long mills) {
    try {
      Thread.sleep(mills);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 数据解压缩
   *
   * @param data
   */
  public static byte[] decompress(byte[] data) throws Exception {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

    // 解压缩
    decompress(inputStream, outputStream);
    data = outputStream.toByteArray();
    outputStream.flush();
    outputStream.close();
    inputStream.close();
    return data;
  }


  /**
   * 数据解压缩
   *
   * @param is
   * @param os
   */
  public static void decompress(InputStream is, OutputStream os) throws Exception {
    GZIPInputStream gis = new GZIPInputStream(is);
    int count;
    byte[] data = new byte[BUFFER];
    while ((count = gis.read(data, 0, BUFFER)) != -1) {
      os.write(data, 0, count);
    }
    gis.close();
  }

  @SuppressWarnings("unchecked")
  public static <T extends BaseEnum> T getEnumByCode(String code, Class<T> enumClass) {
    if (StringUtils.isBlank(code) || enumClass == null || !enumClass.isEnum()) {
      return null;
    }

    Map<String, BaseEnum> targetEnumMap = ENUM_MAP.computeIfAbsent(enumClass, var -> {
      T[] enumConstants = enumClass.getEnumConstants();
      Map<String, BaseEnum> classEnumMap = new ConcurrentHashMap<>();
      for (T enumConstant : enumConstants) {
        classEnumMap.put(enumConstant.code(), enumConstant);
      }
      return classEnumMap;
    });

    return (T) targetEnumMap.get(code);
  }

  public static boolean isArrayMessage(String message) {
    if (StringUtils.isBlank(message)) {
      return false;
    }
    return message.charAt(0) == ARRAY_MESSAGE_PREFIX;
  }

  public static byte[] saveArrayToPng(String title, Map<String, Float> xy) throws IOException {
    DefaultCategoryDataset dataset = new DefaultCategoryDataset();
    xy.forEach((x, y) -> dataset.addValue(y, "Series1", x));

    JFreeChart lineChart = ChartFactory.createLineChart(
        title,
        "Index",
        "Value",
        dataset
    );

    CategoryPlot plot = (CategoryPlot) lineChart.getPlot();
    plot.setDomainGridlinesVisible(true);

    int width = 1024;
    int height = 768;
    File tempFile = File.createTempFile("arraypng", ".png");
    try {
      ChartUtils.saveChartAsPNG(tempFile, lineChart, width, height);
      byte[] bytes = Files.readAllBytes(tempFile.toPath());
      tempFile.delete();
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
