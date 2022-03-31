package com.example.kafka.spring;

import org.springframework.boot.SpringApplication;
import com.example.kafka.spring.services.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

@SpringBootApplication
public class Application {
    private ProducerService producerService;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Autowired
    void Application(ProducerService producerService) {
        this.producerService = producerService;
    }

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
}
