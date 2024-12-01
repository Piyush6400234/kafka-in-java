package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CreateTopic {
    private AdminClient adminClient;

    public CreateTopic(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        this.adminClient = AdminClient.create(props);
    }
    public void createTopic(String topicName, int numPartitions, short replicationFactor) {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);

        try {
            adminClient.createTopics(Collections.singleton(newTopic)).all().get();
            System.out.printf("Topic '%s' created successfully with %d partitions.%n", topicName, numPartitions);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException) {
                System.out.printf("Topic '%s' already exists.%n", topicName);
            } else {
                System.err.printf("Error creating topic '%s': %s%n", topicName, e.getMessage());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Thread was interrupted during topic creation.");
        }
    }

    public void close() {
        adminClient.close();
    }
}
