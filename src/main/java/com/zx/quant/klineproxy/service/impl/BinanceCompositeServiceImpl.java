package com.zx.quant.klineproxy.service.impl;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.zx.quant.klineproxy.client.BinanceCompositeClient;
import com.zx.quant.klineproxy.model.CompositeArticleCatalogs;
import com.zx.quant.klineproxy.model.CompositeArticles;
import com.zx.quant.klineproxy.model.CompositeResponse;
import com.zx.quant.klineproxy.service.CompositeService;
import com.zx.quant.klineproxy.util.ClientUtil;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * composite service impl
 * @author flamhaze5946
 */
@Slf4j
@Service("binanceCompositeService")
public class BinanceCompositeServiceImpl implements CompositeService {

  private final LoadingCache<CmsArticlesKey, CompositeResponse<CompositeArticles>> articlesCache = Caffeine.newBuilder()
      .maximumSize(64)
      .expireAfterWrite(Duration.ofMinutes(5))
      .build(this::queryCmsArticles0);

  private final LoadingCache<CmsArticleCatalogsKey, CompositeResponse<CompositeArticleCatalogs>> articleCatalogsCache = Caffeine.newBuilder()
      .maximumSize(64)
      .expireAfterWrite(Duration.ofMinutes(5))
      .build(this::queryCmsArticleCatalogs0);

  @Autowired
  private BinanceCompositeClient binanceCompositeClient;

  @Override
  public CompositeResponse<CompositeArticles> queryCmsArticleCatalogs(String catalogId, Integer pageNo, Integer pageSize) {
    CmsArticlesKey cmsArticlesKey = new CmsArticlesKey(catalogId, pageNo, pageSize);
    return articlesCache.get(cmsArticlesKey);
  }

  @Override
  public CompositeResponse<?> queryCmsArticles(String catalogId, Integer type, Integer pageNo,
      Integer pageSize) {
    CmsArticleCatalogsKey cmsArticleCatalogsKey = new CmsArticleCatalogsKey(catalogId, type, pageNo, pageSize);
    return articleCatalogsCache.get(cmsArticleCatalogsKey);
  }

  private CompositeResponse<CompositeArticles> queryCmsArticles0(CmsArticlesKey key) {
    return ClientUtil.getResponseBody(
        binanceCompositeClient.getCmsArticles(key.getCatalogId(), key.getPageNo(), key.getPageSize()));
  }

  private CompositeResponse<CompositeArticleCatalogs> queryCmsArticleCatalogs0(CmsArticleCatalogsKey key) {
    return ClientUtil.getResponseBody(
        binanceCompositeClient.getCmsArticleCatalogs(key.getCatalogId(), key.getType(), key.getPageNo(), key.getPageSize()));
  }

  @Getter
  @AllArgsConstructor
  @EqualsAndHashCode
  private static class CmsArticlesKey {

    private String catalogId;

    private Integer pageNo;

    private Integer pageSize;
  }

  @Getter
  @AllArgsConstructor
  @EqualsAndHashCode
  private static class CmsArticleCatalogsKey {

    private String catalogId;

    private Integer type;

    private Integer pageNo;

    private Integer pageSize;
  }
}
