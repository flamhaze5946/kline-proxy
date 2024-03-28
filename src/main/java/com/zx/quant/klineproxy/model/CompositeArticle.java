package com.zx.quant.klineproxy.model;

import lombok.Data;

/**
 * composite article
 * @author flamhaze5946
 */
@Data
public class CompositeArticle {

  private Long id;

  private String code;

  private String title;

  private Object imageLink;

  private Object shortLink;

  private Object body;

  private Object type;

  private Object catalogId;

  private Object catalogName;

  private Object publishDate;

  private Object footer;

  private Long releaseDate;
}
