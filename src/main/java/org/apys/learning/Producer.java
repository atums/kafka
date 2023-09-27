package org.apys.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
        final String TOPIC_NAME = "demo-topic";
        final int MESSAGES_NUMBER = 10000;

        // Initialize Logger
        Logger logger = LoggerFactory.getLogger(Producer.class);

        // Create Producer properties
        Properties properties = new Properties();
        //Адрес Брокера
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        //Реализация ключей сообщений
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //Реализация строк сообщений (самих сообщений)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Собственно создаем Продюсер где ключи это String и сообщения строковые (для Кафка это все равно байты)
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Generate messages
        for (int i = 0; i < MESSAGES_NUMBER; i++) {
            //Создаем сообщение
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    TOPIC_NAME,
                    "key_" + i % 3,
                    String.valueOf(i)
            );

            //Отправляем созданное сообщение ((metadata, exception) - получаем уведомление об отправке)
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("received new metadata, topic: " + metadata.topic()
                            + " partition: " + metadata.partition()
                            + " offsets: " + metadata.offset()
                            + " timestamp: " + metadata.timestamp());
                } else {
                    logger.error("error producing: ", exception);
                }
            });

            // Observing Kafka round-robin feature
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // Flush data and close producer
        producer.flush();
        producer.close();
    }

}
