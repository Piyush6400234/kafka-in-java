package org.example;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.*;
public class Producer {
    private KafkaProducer<String, String> producer;

    public Producer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String key, PayloadObject value) {
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            String jsonMessage = objectMapper.writeValueAsString(value);
            System.out.println("JsonMessage "+ jsonMessage);
            StringBuilder s = new StringBuilder(key);
            s.replace(0, 3, "");
            int target_partition = 0;

            if(!value.getId().equals("fail"))
            {
                String p = s.toString();
                target_partition = Integer.parseInt(p) % 3;
                System.out.println("partition sent to" + target_partition);
            }
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, target_partition, key, jsonMessage);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Message sent to topic: %s, partition: %d, offset: %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        } catch (Exception e){
            System.out.println("could not send message to queue.");
        }

    }

    public void close() {
        producer.close();
    }

}
