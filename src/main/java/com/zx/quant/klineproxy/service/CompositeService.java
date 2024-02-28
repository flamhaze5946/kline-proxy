package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.CompositeResponse;

/**
 * composite service
 * @author flamhaze5946
 */
public interface CompositeService {

  /**
   * query cms articles
   * @param catalogId catalog id
   * @param pageNo    page number
   * @param pageSize  page size
   * @return cms articles
   */
  CompositeResponse<?> queryCmsArticles(String catalogId, Integer pageNo, Integer pageSize);
}
