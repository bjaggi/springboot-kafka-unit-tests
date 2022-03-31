package com.example.kafka.spring.services.utils;

import com.example.kafka.spring.services.ProducerServiceTest;
import com.example.kafka.spring.avro.person;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.IOException;
import java.util.Map;


public class CustomKafkaAvroSerializer extends KafkaAvroSerializer {

    public CustomKafkaAvroSerializer() {
        super();
        this.schemaRegistry = CustomKafkaAvroDeserializer.getMockClient(person.SCHEMA$);
        try {
            this.schemaRegistry.register(ProducerServiceTest.INPUT_TOPIC +"-value", person.SCHEMA$);
            this.schemaRegistry.register(ProducerServiceTest.OUTPUT_TOPIC +"-value", person.SCHEMA$);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
    }

    public CustomKafkaAvroSerializer(SchemaRegistryClient client) {
        super(new MockSchemaRegistryClient());
    }

    public CustomKafkaAvroSerializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(new MockSchemaRegistryClient(), props);
    }

}
