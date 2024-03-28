package com.zx.quant.klineproxy.model;

import java.util.List;
import lombok.Data;

/**
 * composite article
 * @author flamhaze5946
 */
@Data
public class CompositeArticleCatalog {

  private Long catalogId;

  private Long parentCatalogId;

  private String icon;

  private String catalogName;

  private String description;

  private Integer catalogType;

  private Long total;

  private List<CompositeArticle> articles;
}
