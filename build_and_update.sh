mvn clean package
docker build -t flamhaze5946/kline-proxy:1.4.6 --platform linux/amd64 .
docker push flamhaze5946/kline-proxy:1.4.6
