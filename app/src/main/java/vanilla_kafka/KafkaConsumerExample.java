package vanilla_kafka;

import java.time.Duration;
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
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Configuraciones adicionales para controlar el consumo
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10); // Queremos lotes de 10
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Control manual

        // --- CONFIGURACIÓN DE SEGURIDAD SSL ---
        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location", "C:/Users/PC/Desktop/Kafka/certificados/kafka.truststore.jks");
        props.put("ssl.truststore.password", "SISTEMASCENTRALESkey");

        // Crear el consumidor de Kafka
        Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Suscribirse al topic
        consumer.subscribe(Collections.singletonList("ex-topic"));

        // Queremos procesar un total de 50 mensajes
        int mensajesAProcesar = 20;
        int contador = 0;

        // Recibir mensajes
        try {
            while (contador < mensajesAProcesar) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {
                    System.out.println("No hay más mensajes por ahora...");
                    break; // O podrías seguir esperando
                }

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("--- ¡Mensaje Recibido! ---");
                    System.out.println("Clave: " + record.key());
                    System.out.println("Valor: " + record.value()); // Aquí verías el "Hola Kafka"
                    System.out.println("Partición: " + record.partition());
                    System.out.println("Offset: " + record.offset());

                    contador++;

                    // Si llegamos al límite dentro del lote, dejamos de procesar
                    if (contador >= mensajesAProcesar)
                        break;
                }

                // --- EL PASO CRUCIAL: COMMIT POR LOTE ---
                // Confirmamos a Kafka que ya procesamos este grupo de mensajes
                consumer.commitSync();
                System.out.println("Lote confirmado en el broker.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
