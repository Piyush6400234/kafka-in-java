package org.example;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String topic = "partition-topic";
        String deadLetterTopic = "dead-letter-topic";

        CreateTopic topicCreator = new CreateTopic(bootstrapServers);
        topicCreator.createTopic("partition-topic", 3, (short) 1);
        topicCreator.createTopic("dead-letter-topic", 1, (short)1);

        // Run the Producer in a separate thread
        new Thread(() -> {
            Producer producer = new Producer(bootstrapServers);
            for (int i = 1; i <= 15; i++) {
                if(i%10 == 0){
                    PayloadObject p2 = new PayloadObject("Piyush"+1, "piyush"+i*23+"@gmail.com", i*85+"85421"+i*985, "fail");
                    producer.sendMessage(topic, "key" + i, p2);
                }
                else{
                    PayloadObject p1 = new PayloadObject("Piyush"+1, "piyush"+i*23+"@gmail.com", i*85+"85421"+i*985, ""+i*2);
                    producer.sendMessage(topic, "key" + i, p1);
                }

                try {
                    Thread.sleep(1000); // Simulate delay between messages
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            producer.close();
        }).start();

        // Run the Consumer in a separate thread
        new Thread(() -> {
            Producer producer = new Producer(bootstrapServers);
            Consumer consumer = new Consumer(bootstrapServers, "group-A", producer, deadLetterTopic);
            consumer.subscribeToTopic(topic);
            consumer.consumeMessages();
        }).start();

    }
}