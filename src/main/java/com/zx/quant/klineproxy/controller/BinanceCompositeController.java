package com.zx.quant.klineproxy.controller;

import com.zx.quant.klineproxy.model.CompositeResponse;
import com.zx.quant.klineproxy.service.CompositeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * binance future controller
 * @author flamhaze5946
 */
@RestController
@RequestMapping("bapi/composite")
public class BinanceCompositeController extends GenericController {

  @Autowired
  @Qualifier("binanceCompositeService")
  private CompositeService compositeService;

  @GetMapping("v1/public/cms/article/catalog/list/query")
  public CompositeResponse<?> queryCmsArticles(
      @RequestParam("catalogId") String catalogId,
      @RequestParam("pageNo") Integer pageNo,
      @RequestParam("pageSize") Integer pageSize
  ) {
    return compositeService.queryCmsArticles(catalogId, pageNo, pageSize);
  }
}

