FROM openjdk:17-alpine

ENV JAVA_OPTS="-Xms800m"
COPY target/kline-proxy-1.1.0.jar /app/
VOLUME ["/app/application.yaml"]

WORKDIR /app
EXPOSE 1888

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar kline-proxy-1.1.0.jar --spring.config.location=file:/app/application.yaml"]