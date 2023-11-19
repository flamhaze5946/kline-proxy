package com.zx.quant.klineproxy.config;

import com.zx.quant.klineproxy.client.ws.client.AbstractWebSocketClient;
import com.zx.quant.klineproxy.client.ws.client.BinanceFutureWebSocketClient;
import com.zx.quant.klineproxy.client.ws.client.BinanceSpotWebSocketClient;
import com.zx.quant.klineproxy.client.ws.client.WebSocketClient;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * websocket client config
 * @author flamhaze5946
 */
@Configuration
public class WebSocketClientConfig {

  @Bean
  public static ClientRegisterProcessor<BinanceSpotWebSocketClient> binanceSpotWebSocketClientBeanProcessor() {
    return new ClientRegisterProcessor<>(BinanceSpotWebSocketClient.class);
  }

  @Bean
  public static ClientRegisterProcessor<BinanceFutureWebSocketClient> binanceFutureWebSocketClientBeanProcessor() {
    return new ClientRegisterProcessor<>(BinanceFutureWebSocketClient.class);
  }

  /**
   * client register processor
   * @author flamhaze5946
   */
  @Slf4j
  private static class ClientRegisterProcessor<T extends AbstractWebSocketClient<?>> implements BeanFactoryPostProcessor {

    private static final String BEAN_NAME_SEP = "_";

    private static final int BEAN_COUNT = 30;

    private final Class<T> clazz;

    private ClientRegisterProcessor(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void postProcessBeanFactory(@NotNull ConfigurableListableBeanFactory beanFactory)
        throws BeansException {
      for(int i = 0; i < BEAN_COUNT; i++) {
        String beanName = String.join(BEAN_NAME_SEP, clazz.getSimpleName(), String.valueOf(i));
        int clientNumber = i;
        BeanDefinition beanDefinition = new RootBeanDefinition(clazz, () -> {
          try {
            Constructor<T> constructor = clazz.getDeclaredConstructor(int.class);
            return constructor.newInstance(clientNumber);
          } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                   IllegalAccessException e) {
            throw new RuntimeException(e);
          }
        });
        ((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(
            beanName, beanDefinition);
      }
    }
  }
}