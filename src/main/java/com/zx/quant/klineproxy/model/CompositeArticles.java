package com.zx.quant.klineproxy.model;

import java.util.List;
import lombok.Data;

/**
 * composite articles
 * @author flamhaze5946
 */
@Data
public class CompositeArticles {

  private List<CompositeArticle> articles;

  private Long total;
}
