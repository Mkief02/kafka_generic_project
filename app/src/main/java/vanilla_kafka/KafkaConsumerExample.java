package vanilla_kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        
        // Configuración del consumidor de Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // --- CONFIGURACIÓN DE SEGURIDAD SSL ---
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:/Users/PC/Desktop/Kafka/certificados/kafka.truststore.jks");
        props.put("ssl.truststore.password", "SISTEMASCENTRALESkey");


        
        // Crear el consumidor de Kafka
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Suscribirse al topic
        consumer.subscribe(Collections.singletonList("test-topic"));

        // Recibir mensajes
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }

        }
    
    }
}
