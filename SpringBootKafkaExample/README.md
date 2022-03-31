# Kafa - Spring

## Getting started:

1. Open `application.yml` and fill in the required config variables.
2. Run `$ ./mvnw clean install`

## Application:

This application has a Producer with a rest endpoint that allows the user to submit messages using a rest client,
and a Consumer that is always listening and polling the messages from the desired topic.

1. Open `ProducerService.java` and fill in the `TOPIC`.
2. Open `ConsumerService.java` and fill in the `TOPIC` and `GROUP_ID`.
3. Run `$ ./mvnw spring-boot:run`.
