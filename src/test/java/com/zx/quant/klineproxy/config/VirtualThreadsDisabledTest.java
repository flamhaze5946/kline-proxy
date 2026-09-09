package com.zx.quant.klineproxy.config;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;

/**
 * "It is on" is only half the claim: with the flag absent the server must stay on the platform
 * worker pool, so the jar can ship dark and the threading model be switched separately from the
 * container swap.
 *
 * <p>Also pins down that the container really is Tomcat now — {@code http-nio-*} is a Tomcat
 * worker name, {@code XNIO-*} was Undertow's.
 *
 * @author flamhaze5946
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    classes = ServerThreadingProbe.MinimalServer.class)
class VirtualThreadsDisabledTest {

  @LocalServerPort
  int port;

  @Test
  void stays_on_platform_tomcat_threads_when_the_flag_is_absent() throws Exception {
    String body = ServerThreadingProbe.call(port);
    assertFalse(body.startsWith("true "), "handler thread was virtual without the flag: " + body);
    assertTrue(body.contains("http-nio-"), "not on a Tomcat worker thread: " + body);
  }
}
