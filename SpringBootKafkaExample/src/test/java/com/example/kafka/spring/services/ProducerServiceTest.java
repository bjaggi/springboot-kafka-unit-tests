package com.example.kafka.spring.services;

import com.example.kafka.spring.avro.person;
import com.example.kafka.spring.services.utils.CustomKafkaAvroDeserializer;
import com.example.kafka.spring.services.utils.CustomKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDate;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@RunWith(SpringRunner.class)
@DirtiesContext(methodMode = DirtiesContext.MethodMode.BEFORE_METHOD)
@SpringBootTest
@EmbeddedKafka(partitions = 1,
ports = 9092)
public class ProducerServiceTest {
    public static final String INPUT_TOPIC = "input-topic";
    public static final String OUTPUT_TOPIC = "output-topic";

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    ConsumerService consumerService;

    @Autowired
    ProducerService producerService;

    Producer<String, person> producer1;
    Producer<String, person> producer2;
    Consumer<String, person> inputTopicConsumer;
    Consumer<String, person> outputTopicConsumer;

    private person test_message =   person.newBuilder().setId("testId").setFirstName("firstName")
            .setLastName("lastName").setDateOfBirth(LocalDate.parse("1990-01-01").toEpochDay())
            .setLang("English").build();

    private ConsumerFactory<String, person> configureConsumerFactory(String group) {

        return new DefaultKafkaConsumerFactory(consumerConfigs(group), new StringDeserializer(), new CustomKafkaAvroDeserializer());
    }

    @Bean
    public Map<String, Object> consumerConfigs(String group) {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.consumerProps(group, "false", embeddedKafkaBroker));
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8080");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.embeddedKafkaBroker.getBrokersAsString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    private ProducerFactory<String, person> configureProducerFactory() {
        return new DefaultKafkaProducerFactory(producerConfigs(), new StringSerializer(), new CustomKafkaAvroSerializer());
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8080");
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, true);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.embeddedKafkaBroker.getBrokersAsString());
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return props;
    }

    @Bean
    public KafkaTemplate<String, person> kafkaTemplate() {
        KafkaTemplate<String, person> kafkaTemplate = new KafkaTemplate<String, person>(configureProducerFactory());
        return kafkaTemplate;
    }

    @Before
    public void setUp() throws Exception {
        producer1 = configureProducer();
        producer2 = configureProducer();
        inputTopicConsumer = configureConsumer("test-group1");
        outputTopicConsumer = configureConsumer("test-group2");
        inputTopicConsumer.subscribe(Collections.singleton(INPUT_TOPIC));
        outputTopicConsumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
    }

    @After
    public void tearDown() throws Exception {
        producer1.close();
        producer2.close();
        inputTopicConsumer.close();
        outputTopicConsumer.close();
    }

   private Producer<String, person> configureProducer() {
        return configureProducerFactory().createProducer();
    }

    private Consumer<String, person> configureConsumer(String group) {
        return configureConsumerFactory(group).createConsumer();
    }

    @Test
    public void testWriteToOneTopicAndConsumerReadAndWriteToAnotherTopicUSsingEmbeddedKafka() {
        inputTopicConsumer.subscribe(Collections.singleton(INPUT_TOPIC));
        producer1.send(new ProducerRecord<>(INPUT_TOPIC, "en", test_message));

        Iterator<ConsumerRecord<String, person>> inputTopicItr = KafkaTestUtils.getRecords(inputTopicConsumer).records(INPUT_TOPIC).iterator();
        if (inputTopicItr.hasNext()) {
            ConsumerRecord<String, person> singleRecord = inputTopicItr.next();
            Assertions.assertThat(singleRecord).isNotNull();
            Assertions.assertThat(singleRecord.key()).isEqualTo("en");
            Assertions.assertThat(singleRecord.value().getFirstName()).isEqualTo(test_message.getFirstName());
            Assertions.assertThat(singleRecord.value().getLang()).isEqualTo(test_message.getLang());
        }

        inputTopicConsumer.subscribe(Collections.singleton(OUTPUT_TOPIC));

        Iterator<ConsumerRecord<String, person>> outputTopicItr = KafkaTestUtils.getRecords(inputTopicConsumer).records(OUTPUT_TOPIC).iterator();
        if(outputTopicItr.hasNext()) {
            ConsumerRecord<String, person> singleRecord1 = outputTopicItr.next();
            Assertions.assertThat(singleRecord1).isNotNull();
            Assertions.assertThat(singleRecord1.key()).isEqualTo("en");
            Assertions.assertThat(singleRecord1.value().getFirstName()).isEqualTo(test_message.getFirstName());
            Assertions.assertThat(singleRecord1.value().getLang()).isEqualTo(test_message.getLang());
        }
    }

    @Test
    public void testWriteToOneTopicAndConsumerReadAndWriteToAnotherTopicUsingServices() {
        //Subscribe to INPUT_TOPIC topic and send message using producerService
        inputTopicConsumer.subscribe(Collections.singleton(INPUT_TOPIC));
        producerService.sendMessageToTopic("en", test_message, INPUT_TOPIC, kafkaTemplate());

        //read message from INPUT_TOPIC topic
        Iterator<ConsumerRecord<String, person>> inputTopicItr = KafkaTestUtils.getRecords(inputTopicConsumer).records(INPUT_TOPIC).iterator();
        if(inputTopicItr.hasNext()) {
            ConsumerRecord<String, person> singleRecord = inputTopicItr.next();
            Assertions.assertThat(singleRecord).isNotNull();
            Assertions.assertThat(singleRecord.key()).isEqualTo("en");
            Assertions.assertThat(singleRecord.value().getFirstName()).isEqualTo(test_message.getFirstName());
            Assertions.assertThat(singleRecord.value().getLang()).isEqualTo(test_message.getLang());
            Assertions.assertThat(singleRecord.value().getGreetings()).isEqualTo(null);
        }

        //Subscribe to OUTPUT_TOPIC topic
        outputTopicConsumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
        // ConsumerService class already consume service and write to OUTPUT_TOPIC topic. So ideally we should be able to run test without sending message from here
        consumerService.setGreetingAndsendMessageToTopic("en", test_message, kafkaTemplate(), OUTPUT_TOPIC);

        // read message from OUTPUT_TOPIC topic
        //ConsumerRecord<String, person> record = KafkaTestUtils.getSingleRecord(inputTopicConsumer, OUTPUT_TOPIC);
        Iterator<ConsumerRecord<String, person>> outputTopicItr = KafkaTestUtils.getRecords(outputTopicConsumer).records(OUTPUT_TOPIC).iterator();
        if(outputTopicItr.hasNext()) {
            ConsumerRecord<String, person> enTopicRecord = outputTopicItr.next();
            Assertions.assertThat(enTopicRecord).isNotNull();
            Assertions.assertThat(enTopicRecord.value().getLang()).isEqualTo("English");
            Assertions.assertThat(enTopicRecord.value().getGreetings()).isEqualTo("HI!");
        }
    }
}