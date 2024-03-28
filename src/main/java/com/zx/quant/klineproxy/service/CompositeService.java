package com.zx.quant.klineproxy.service;

import com.zx.quant.klineproxy.model.CompositeResponse;

/**
 * composite service
 * @author flamhaze5946
 */
public interface CompositeService {

  /**
   * query cms article catalogs
   * @param catalogId catalog id
   * @param pageNo    page number
   * @param pageSize  page size
   * @return cms articles
   */
  CompositeResponse<?> queryCmsArticleCatalogs(String catalogId, Integer pageNo, Integer pageSize);

  /**
   * query cms articles
   * @param catalogId catalog id
   * @param type      type
   * @param pageNo    page number
   * @param pageSize  page size
   * @return cms articles
   */
  CompositeResponse<?> queryCmsArticles(String catalogId, Integer type, Integer pageNo, Integer pageSize);
}
