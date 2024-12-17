package com.zx.quant.klineproxy.client;

import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.http.GET;

/**
 * block chain center client
 * @author flamhaze5946
 */
public interface BlockChainCenterClient {

  @GET("en/altcoin-season-index")
  Call<ResponseBody> getRawAltCoinSeasonIndex();
}
