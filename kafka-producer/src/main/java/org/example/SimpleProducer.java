package org.example;

import org.apache.kafka.clients.producer.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducer {

    private final Producer<String, String> producer;
    final String outTopic;

    public SimpleProducer(final Producer<String, String> producer,
            final String topic) {
        this.producer = producer;
        outTopic = topic;
    }

    public Future<RecordMetadata> produce(final String message) {
        final String[] parts = message.split("-");
        final String key, value;
        if (parts.length > 1) {
            key = parts[0];
            value = parts[1];
        } else {
            key = null;
            value = parts[0];
        }
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(outTopic, key, value);
        return producer.send(producerRecord);
    }

    public void shutdown() {
        producer.close();
    }

    public static Properties loadProperties(String fileName) throws IOException {
        final Properties envProps = new Properties();
        final FileInputStream input = new FileInputStream(fileName);
        envProps.load(input);
        input.close();
        return envProps;
    }

    public static void main(String[] args) throws Exception {
        final Properties props = loadProperties("kafka.properties");
        final String topic = "input";
        final Producer<String, String> producer = new KafkaProducer<>(props);
        final SimpleProducer producerApp = new SimpleProducer(producer, topic);

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(System.in));
        try {
            while (true) {
                String message = reader.readLine();
                if (message.equals("exit"))
                    break;
                producerApp.produce(message);
                System.out.println("Sent Message");

            }
        } catch (IOException e) {
            System.err.printf("Error %s", e);
        } finally {
            producerApp.shutdown();
        }
    }

}
