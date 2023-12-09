mvn clean package
docker build -t flamhaze5946/kline-proxy:1.0.0 --platform linux/amd64 .
docker push flamhaze5946/kline-proxy:1.0.0