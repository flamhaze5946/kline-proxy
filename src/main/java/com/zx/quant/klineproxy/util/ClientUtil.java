package com.zx.quant.klineproxy.util;

import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.ResponseBody;
import retrofit2.Call;
import retrofit2.Response;

/**
 * client util
 * @author flamhaze5946
 */
@Slf4j
public final class ClientUtil {

  private static final String DEFAULT_ERROR_MSG = "client call failed.";

  public static <T> T getResponseBody(Call<T> call) {
    return getResponseBody(call, false, null);
  }

  public static <T> T getResponseBody(Call<T> call, Runnable onOverRate) {
    return getResponseBody(call, false, onOverRate);
  }

  public static <T> T getResponseBody(Call<T> call, boolean bodyAllowNull, Runnable onOverRate) {
    Response<T> response = null;
    try {
      response = call.execute();
      int code = response.code();
      if (!response.isSuccessful()) {
        if (code == 418 || code == 429) {
          if (onOverRate != null) {
            onOverRate.run();
          }
        }
        String errorMsg = DEFAULT_ERROR_MSG;
        try(ResponseBody errorBody = response.errorBody()) {
          if (errorBody != null) {
            String errorString = errorBody.string();
            errorMsg = errorString;
            log.warn("call for response failed, code: {}, error body: {}", code, errorString);
          }
          throw new RuntimeException(errorMsg);
        }
      }
      T body = response.body();
      if (!bodyAllowNull && body == null) {
        throw new RuntimeException("body from call: {} is null.");
      }
      return body;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
