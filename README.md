# kafka-message-reprocess-api #

## Prequisite ##

- Install kafka cluster
- Start the zookeeper and kafka cluster using below commands:
  
  ```sh
      cd /kafka_version
      sudo bin/zookeeper-server-start.sh config/zookeeper.properties
      sudo bin/kafka-server-start.sh config/server.properties
  ```
  
- Create the topic using below commands:
  
  ```sh
      cd /kafka_version
      sudo bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic [TOPIC-NAME] --create --partitions 2 --replication-factor 1
  ```
  
- Publish the messages into topic:

  ```sh
      cd /kafka_version
      bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic [TOPIC-NAME]
  ```

## Steps to start the application ##

- Configure the created topic in application.properties
- Now start the application

``` sh
    mvn spring-boot:run
```

- Open the swagger UI in browser

```sh
  http://localhost:8080/swagger-ui.html
```





