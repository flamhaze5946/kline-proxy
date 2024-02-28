package com.zx.quant.klineproxy.client;

import com.zx.quant.klineproxy.model.CompositeArticles;
import com.zx.quant.klineproxy.model.CompositeResponse;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Query;

public interface BinanceCompositeClient {

  @GET("bapi/composite/v1/public/cms/article/catalog/list/query")
  Call<CompositeResponse<CompositeArticles>> getCmsArticles(
      @Query("catalogId") String catalogId,
      @Query("pageNo") Integer pageNo,
      @Query("pageSize") Integer pageSize
  );
}
