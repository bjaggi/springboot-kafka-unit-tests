package com.example.kafka.spring.controllers;

import java.time.LocalDate;

import com.example.kafka.spring.avro.person;
import com.example.kafka.spring.services.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {
    @Autowired
    ProducerService producerService;


    @PostMapping("/add")
    public void add(@RequestParam String firstName, @RequestParam String lastName, @RequestParam String dateOfBirth) {
        String id = "12345";
        String phoneNumber = "1234567890";
        String lang = "en";

        person personRecord = person.newBuilder().setId(id).setFirstName(firstName).setLastName(lastName)
                .setDateOfBirth(LocalDate.parse(dateOfBirth).toEpochDay()).setPhoneNumber(phoneNumber).setLang(lang).build();

        //producerService.sendMessage(null, personRecord);
    }
}
