package com.example.kafka.spring.services.utils;

import com.example.kafka.spring.services.ProducerServiceTest;
import com.example.kafka.spring.avro.person;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;

import java.io.IOException;


public class CustomKafkaAvroDeserializer extends KafkaAvroDeserializer {

    @Override
    public Object deserialize(String topic, byte[] bytes) {
        this.schemaRegistry = getMockClient(person.SCHEMA$);
        try {
            this.schemaRegistry.register(ProducerServiceTest.INPUT_TOPIC +"-value", person.SCHEMA$);
            this.schemaRegistry.register(ProducerServiceTest.OUTPUT_TOPIC +"-value", person.SCHEMA$);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (RestClientException e) {
            e.printStackTrace();
        }
        return super.deserialize(topic, bytes);
    }

    public static SchemaRegistryClient getMockClient(final Schema schema$) {
        return new MockSchemaRegistryClient() {
            @Override
            public synchronized Schema getById(int id) {
                return schema$;
            }
        };
    }
}
