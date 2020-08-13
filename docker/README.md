# All common services 

In order to spin it up just run `docker-compose up` and you'll get a Kakfa and Zookeeper container. 

----------

#### Find the container name
`$docker ps`

`CONTAINER NAME:docker_kafka_1`

-----------

#### List the topics

`$ docker exec docker_kafka_1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`


#### Producer CLI command

`docker exec -it docker_kafka_1 /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic word-count-input-topic`


#### Consumer CLI command
 
`docker exec -it docker_kafka_1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic word-count-output-topic --from-beginning --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer`



