package com.zx.quant.klineproxy.model.config;

import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * kline persistence config properties
 * @author flamhaze5946
 */
@Data
@ConfigurationProperties(prefix = "kline.persistence")
public class KlinePersistenceProperties {

  private boolean enabled = false;

  private boolean loadOnStartup = true;

  private boolean dumpOnShutdown = true;

  private int dumpIntervalSeconds = 300;

  private String rootDir = "./data/kline-cache";

  private ServicePersistenceConfig spot = new ServicePersistenceConfig();

  private ServicePersistenceConfig future = new ServicePersistenceConfig();

  @Data
  public static class ServicePersistenceConfig {

    private Map<String, IntervalPersistenceConfig> intervalConfigs = new HashMap<>();
  }

  @Data
  public static class IntervalPersistenceConfig {

    private Integer maxStoreCount;

    private Map<String, Integer> symbolMaxStoreCounts = new HashMap<>();
  }
}
