package dev.sstol.srv3;

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
public class Srv3Application {

   public static void main(String[] args) {
      SpringApplication.run(Srv3Application.class, args);
   }

   @Autowired
   KafkaTemplate<UUID, UserSurnameDto> kafkaTemplate;

   @KafkaListener(topics = "request-usersurname-topic", groupId = "microservice3-group")
   void listen(ConsumerRecord<UUID, Long> record) {
      UUID correlationId = record.key();
      Long userId = record.value();

      kafkaTemplate.send("response-usersurname-topic", correlationId, new UserSurnameDto("Usersurname" + userId));
   }
}

record UserSurnameDto(String surname) {}

@Configuration
class KafkaConfig {
   @Bean
   public ProducerFactory<UUID, UserSurnameDto> producerFactory() {
      return new DefaultKafkaProducerFactory<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSurnameDtoSerializer.class
      ));
   }

   @Bean
   public KafkaTemplate<UUID, UserSurnameDto> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory());
   }
}