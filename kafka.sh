bin/kafka-topics.sh --alter --zookeeper localhost:2181 --topic ECOMMERCE_NEW_ORDER --partitions 3
bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
