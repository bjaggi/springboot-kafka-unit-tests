package com.example.kafka.spring.services;

import java.time.LocalDate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import com.example.kafka.spring.avro.person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;

@Service
public class ConsumerService {
    @Autowired
    ProducerService producerService;

    @Autowired
    KafkaTemplate kafkaTemplate;

    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";
    private static final String GROUP_ID = "test-group";

    @KafkaListener(topics = {INPUT_TOPIC}, groupId = GROUP_ID)
    public void consume(ConsumerRecord<String, person> rec) {
        person record = rec.value();

        System.out.printf("id: %s\n", record.getId());
        System.out.printf("firstName: %s\n", record.getFirstName());
        System.out.printf("lastName: %s\n", record.getLastName());
        System.out.printf("dateOfBirth: %s\n", LocalDate.ofEpochDay(record.getDateOfBirth()));
        System.out.printf("phoneNumber: %s\n", record.getPhoneNumber());

        /*if(record.getLang() != null) {
            setGreetingAndsendMessageToTopic(rec.key(), rec.value(), kafkaTemplate, OUTPUT_TOPIC);
        }*/

    }

    public void setGreetingAndsendMessageToTopic(String key, person person, KafkaTemplate<String, person> template, String topic) {
        person.setGreetings("HI!");
        producerService.sendMessageToTopic(key, person, topic, template);
    }
}
