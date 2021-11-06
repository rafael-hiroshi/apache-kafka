bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic ECOMMERCE_NEW_ORDER --partitions 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
bin/kafka-consumer-groups.sh --all-groups --bootstrap-server localhost:9092 --describe
