package com.example.kafka.spring.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import com.example.kafka.spring.avro.person;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class ProducerService {
    private static final String TOPIC = "input-topic";
    private static final Logger logger = LoggerFactory.getLogger(ProducerService.class);

    @Autowired
    private KafkaTemplate<String, person> kafkaTemplate;

    public void sendMessage(String key, person value) {
        ListenableFuture<SendResult<String, person>> future = kafkaTemplate.send(TOPIC, key, value);
        future.addCallback(new ListenableFutureCallback<SendResult<String, person>>() {
            @Override
            public void onSuccess(SendResult<String, person> result) {
                logger.info(String.format("Produced event to topic %s: key = %-10s value = %s", TOPIC, key, value));
            }

            @Override
            public void onFailure(Throwable ex) {
                ex.printStackTrace();
            }
        });
    }

    public void sendMessageToTopic(String key, person value, String topic, KafkaTemplate<String, person> template) {

        ListenableFuture<SendResult<String, person>> future = template.send(topic, key, value);
        future.addCallback(new ListenableFutureCallback<SendResult<String, person>>() {
            @Override
            public void onSuccess(SendResult<String, person> result) {
                logger.info(String.format("Produced event to topic %s: key = %-10s value = %s", topic, key, value));
            }

            @Override
            public void onFailure(Throwable ex) {
                ex.printStackTrace();
            }
        });
    }
}
