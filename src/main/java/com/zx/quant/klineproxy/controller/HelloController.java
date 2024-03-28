package com.zx.quant.klineproxy.controller;

import com.google.common.collect.Lists;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * hello controller
 * @author flamhaze5946
 */
@RestController
@RequestMapping("hello")
public class HelloController {

  private static final List<String> IP_HEADERS = Lists.newArrayList(
      "X-Forwarded-For", "X-Real-IP", "Forwarded"
  );

  private static final String HELLO_WORLD = "Hello World!";

  /**
   * hello world
   * @return hello world
   */
  @GetMapping("helloWorld")
  public String helloWorld() {
    return HELLO_WORLD;
  }

  @GetMapping("whatsMyIp")
  public String whatsMyIp(HttpServletRequest request) {
    for (String ipHeader : IP_HEADERS) {
      String ipValue = request.getHeader(ipHeader);
      if (StringUtils.isNotBlank(ipValue)) {
        return ipValue;
      }
    }
    return request.getRemoteAddr();
  }
}
