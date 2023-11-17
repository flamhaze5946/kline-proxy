package com.zx.quant.klineproxy.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;
import org.apache.commons.lang3.StringUtils;

/**
 * common util
 * @author flamhaze5946
 */
public final class CommonUtil {

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
}
