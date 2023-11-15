package com.zx.quant.klineproxy.controller;

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

  private static final String HELLO_WORLD = "Hello World!";

  /**
   * hello world
   * @return hello world
   */
  @GetMapping("helloWorld")
  public String helloWorld() {
    return HELLO_WORLD;
  }
}
