package dev.sstol.srv2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.Map;
import java.util.UUID;

@SpringBootApplication
public class Srv2Application {

   public static void main(String[] args) {
      SpringApplication.run(Srv2Application.class, args);
   }

   @Autowired
   KafkaTemplate<UUID, UserNameDto> kafkaTemplate;

   @KafkaListener(topics = "request-username-topic", groupId = "microservice2-group")
   void listen(ConsumerRecord<UUID, Long> record) {
      UUID correlationId = record.key();
      Long userId = record.value();

      kafkaTemplate.send("response-username-topic", correlationId, new UserNameDto("Username" + userId));
   }
}

record UserNameDto(String name) {}

@Configuration
class KafkaConfig {
   @Bean
   public ProducerFactory<UUID, UserNameDto> producerFactory() {
      return new DefaultKafkaProducerFactory<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserNameDtoSerializer.class
      ));
   }

   @Bean
   public KafkaTemplate<UUID, UserNameDto> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory());
   }
}