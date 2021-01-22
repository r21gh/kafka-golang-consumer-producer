docker-compose -f kafka.yml down && docker-compose -f kafka.yml rm -fsv && docker-compose -f kafka.yml up -d
sleep 10
echo "Wait until kafka broker succeeds"
docker exec -ti rgh_kafka kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic avg
go mod vendor
go build -o main .
# CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o main .
./main
