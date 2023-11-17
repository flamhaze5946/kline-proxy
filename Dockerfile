FROM openjdk:17-alpine

COPY target/kline-proxy-1.0.0.jar /app/
VOLUME ["/app/application.yaml"]

WORKDIR /app
EXPOSE 8888

ENTRYPOINT ["java", "-jar", "kline-proxy-1.0.0.jar"]