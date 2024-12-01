package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer {
    private KafkaConsumer<String, String> consumer;
    private Producer dltProducer;
    private String dltTopic;

    public Consumer(String bootstrapServers, String groupId, Producer dltProducer, String dltTopic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(props);
        this.dltProducer = dltProducer;
        this.dltTopic = dltTopic;
    }

    public void subscribeToTopic(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public void consumeMessages() {
        System.out.println("Waiting for messages...");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                try{
                    System.out.println("Trying to consume message"+record.value());
                    ObjectMapper objectMapper = new ObjectMapper();
                    PayloadObject user = objectMapper.readValue(record.value(), PayloadObject.class);
                    System.out.println("--------------------"+user);
                    System.out.println("Consuming "+user);
                    System.out.printf("Consuming message: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
//                    System.out.println("processing message");
                    processMessage(record.key(), record.value());

                }
                catch(Exception e){
                    System.err.printf("Failed to process message: key=%s, value=%s. Sending to DLT.%n",
                            record.key(), record.value());
                    sendToDeadLetterTopic(record);

                }

            }
        }
    }
    private void processMessage(String key, String value) throws Exception {
        // Simulate processing logic (e.g., throw an exception for specific values)
        if ("fail".equals(value)) {
            throw new Exception("Simulated processing failure");
        }
        System.out.printf("Processed message: key=%s, value=%s%n", key, value);
    }

    private void sendToDeadLetterTopic(ConsumerRecord<String, String> record) {
//        ProducerRecord<String, String> dltRecord = new ProducerRecord<>(deadLetterTopic, record.key(), record.value());
        System.out.println("sending message to dltTopic");
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            PayloadObject user = objectMapper.readValue(record.value(), PayloadObject.class);
            dltProducer.sendMessage(dltTopic, record.key(), user);
        }
        catch(Exception e){
            System.out.println("Could not send to DLQ.");
        }

    }


    public void close() {
        consumer.close();
    }
}
