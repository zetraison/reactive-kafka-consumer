# reactive-kafka-consumer
Sample of Spring Boot Kafka consumer.

In this sample used [Confluent Control Center](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html) as a kafka server and for the generation mock data to a topics.

## Download and Start Confluent Platform Using Docker

Download or copy the contents of the Confluent Platform all-in-one Docker Compose file, for example:

```
curl --silent --output docker-compose.yml \
https://raw.githubusercontent.com/confluentinc/cp-all-in-one/6.2.0-post/cp-all-in-one/docker-compose.yml
```
Start Confluent Platform with the -d option to run in detached mode:

```
docker-compose up -d
```
