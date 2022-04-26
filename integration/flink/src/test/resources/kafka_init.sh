kafka-topics --bootstrap-server kafka:9092 --list && \
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic io.openlineage.flink.kafka.input --replication-factor 1 --partitions 1 && \
kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic io.openlineage.flink.kafka.output --replication-factor 1 --partitions 1 && \
kafka-topics --bootstrap-server kafka:9092 --list
