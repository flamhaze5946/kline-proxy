package com.zx.quant.klineproxy.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;

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
    return runnable -> {
      ThreadGroup threadGroup = newThreadGroup(groupName);
      return new Thread(threadGroup, runnable);
    };
  }

  /**
   * build new thread group
   * @param groupName group name
   * @return thread group
   */
  private static ThreadGroup newThreadGroup(String groupName) {
    return new ThreadGroup(groupName);
  }
}
