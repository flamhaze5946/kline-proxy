package com.zx.quant.klineproxy.model;

import java.util.List;
import lombok.Data;

/**
 * list topics
 * @author flamhaze5946
 */
@Data
public class ListTopicsEvent {

  private List<String> result;

  private Long id;
}
