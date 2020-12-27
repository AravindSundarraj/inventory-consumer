package com.group.kaka.inventory.consumer;

import com.group.kaka.inventory.domain.PosInvoice;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
public class InventoryConsumer {

    private static final String BOOTSTRAP_SERVER = "localhost:9095,localhost:9096,localhost:9097";

    private KafkaConsumer<String, PosInvoice> getProperties() {


        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "inventory-group");
        props.setProperty("enable.auto.commit", "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");

        log.info("Setting Kafka Consumer Properties : {} ", props);

        KafkaConsumer<String, PosInvoice> kafkaConsumer = new KafkaConsumer(props);
        return kafkaConsumer;
    }

    public void consumeInventory() {
        KafkaConsumer<String, PosInvoice> kafkaConsumer = getProperties();
        kafkaConsumer.subscribe(Arrays.asList("TOPIC_INVENTORY"));

        log.info("Consume PosInvoice ...");
        List<ConsumerRecord<String, PosInvoice>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, PosInvoice> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            //log.info("Total Records per poll {} ", consumerRecords.count());
            final int minBatchSize = 5;


            for (ConsumerRecord<String, PosInvoice> cr : consumerRecords) {
                // kafka generic ID
                 String id = cr.topic() + "_" + cr.partition() + "_" + cr.offset();
                 log.info("Consumer-Record Id: {} " , id);
                 log.info("Record Key & Value {}  {} " , cr.key() , cr.value());
                buffer.add(cr);

            }
            if (buffer.size() >= minBatchSize) {

                kafkaConsumer.commitSync();
                buffer.clear();
            }
        }
    }


}
