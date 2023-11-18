package com.zx.quant.klineproxy.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;

/**
 * named thread factory
 * @author flamhaze5946
 */
public class ThreadFactoryUtil {

  private static final Map<String, ThreadFactory> THREAD_FACTORY_MAP = new ConcurrentHashMap<>();

  /**
   * get named thread factory
   * @param groupName thread group name
   * @return named thread factory
   */
  public static ThreadFactory getNamedThreadFactory(String groupName) {
    return THREAD_FACTORY_MAP.computeIfAbsent(groupName, var -> newNamedThreadFactory(groupName));
  }

  /**
   * get named thread factory
   * @param groupName thread group name
   * @return named thread factory
   */
  private static ThreadFactory newNamedThreadFactory(String groupName) {
    return new NamedThreadFactory(groupName);
  }

  private static class NamedThreadFactory implements ThreadFactory {

    private static final String NAME_SEP = "-";

    private final AtomicInteger threadNumber = new AtomicInteger(1);

    private final String namePrefix;

    public NamedThreadFactory(String namePrefix) {
      this.namePrefix = namePrefix;
    }

    @Override
    public Thread newThread(@NotNull Runnable runnable) {
      return new Thread(runnable, String.join(NAME_SEP, namePrefix, String.valueOf(threadNumber.getAndIncrement())));
    }
  }
}
