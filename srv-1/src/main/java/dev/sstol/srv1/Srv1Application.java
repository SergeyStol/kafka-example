package dev.sstol.srv1;

import jakarta.servlet.http.HttpServletResponse;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.serialization.UUIDSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@SpringBootApplication
@RestController
public class Srv1Application {

   public static void main(String[] args) {
      SpringApplication.run(Srv1Application.class, args);
   }

   @Autowired
   ApplicationEventPublisher eventPublisher;

   @Autowired
   UserDtoAggregatorService userDtoAggregatorService;

   @GetMapping("/srv-1/{id}")
   @ResponseStatus(HttpStatus.OK)
   public void getSrv(@PathVariable Long id, HttpServletResponse response) {
      UUID uuid = UUID.randomUUID();
      response.addHeader("new-url", "http://localhost:8080/srv-1/request/" + uuid);
      eventPublisher.publishEvent(new GetUserEvent(this, uuid, id));
   }

   @GetMapping("/srv-1/request/{uuid}")
   @ResponseStatus(HttpStatus.OK)
   public UserDto getSrv(@PathVariable UUID uuid) {
      if (!userDtoAggregatorService.isFullyEnriched(uuid)) {
         return new UserDto("", ""); // TODO: return IN_PROGRESS
      }
      return userDtoAggregatorService.getUserDto(uuid);
   }
}

@Builder
record UserDto(String name, String surname) {}
record UserNameDto(String name) {}
record UserSurnameDto(String surname) {}

@Service
class UserDtoAggregatorService {
   private final Map<UUID, UserDtoAggregator> map = new ConcurrentHashMap<>();

   private UserDtoAggregator getOrAddUserDtoAggregator(UUID uuid) {
      UserDtoAggregator userDtoAggregator = map.get(uuid);
      if (userDtoAggregator != null) {
         return userDtoAggregator;
      }
      userDtoAggregator = new UserDtoAggregator();
      map.put(uuid, userDtoAggregator);

      return userDtoAggregator;
   }

   public boolean isFullyEnriched(UUID uuid) {
      UserDtoAggregator userDtoAggregator = map.get(uuid);
      if (userDtoAggregator == null) {
         return false;
      }
      return !(userDtoAggregator.getUserNameDto() == null
             || userDtoAggregator.getUserSurnameDto() == null);
   }

   public void enrichWith(UUID uuid, UserNameDto userNameDto) {
      var userDtoAggregator = getOrAddUserDtoAggregator(uuid);
      if (userDtoAggregator.getUserNameDto() != null) {
         return;
      }
      userDtoAggregator.setUserNameDto(userNameDto);
   }

   public void enrichWith(UUID uuid, UserSurnameDto userSurnameDto) {
      var userDtoAggregator = getOrAddUserDtoAggregator(uuid);
      if (userDtoAggregator.getUserSurnameDto() != null) {
         return;
      }
      userDtoAggregator.setUserSurnameDto(userSurnameDto);
   }

   public UserDto getUserDto(UUID uuid) {
      UserDtoAggregator userDtoAggregator = map.get(uuid);
      return UserDto.builder()
        .name(userDtoAggregator.getUserNameDto().name())
        .surname(userDtoAggregator.getUserSurnameDto().surname())
        .build();
   }
}

@Getter
@Setter
class UserDtoAggregator {
   UserNameDto userNameDto;
   UserSurnameDto userSurnameDto;
}

@Component
@EnableAsync
@RequiredArgsConstructor
class KafkaClient1 {
   private final KafkaTemplate<UUID, Long> kafkaTemplate;
   private final UserDtoAggregatorService userDtoAggregatorService;

   @Async
   @EventListener
   void getUserEventHandler(GetUserEvent getUserEvent) {
      kafkaTemplate.send("request-username-topic", getUserEvent.getUuid(), getUserEvent.getId());
   }

   @KafkaListener(
     topics = "response-username-topic",
     groupId = "microservice2-group",
     containerFactory = "userNameDtoKafkaListenerContainerFactory"
   )
   public void listen(ConsumerRecord<UUID, UserNameDto> response) {
      userDtoAggregatorService.enrichWith(response.key(), response.value());
   }
}

@Component
@EnableAsync
@RequiredArgsConstructor
class KafkaClient2 {
   private final KafkaTemplate<UUID, Long> kafkaTemplate;
   private final UserDtoAggregatorService userDtoAggregatorService;
   @Async
   @EventListener
   void getUserEventHandler(GetUserEvent getUserEvent) {
      kafkaTemplate.send("request-usersurname-topic", getUserEvent.getUuid(), getUserEvent.getId());
   }

   @KafkaListener(
     topics = "response-usersurname-topic",
     groupId = "microservice3-group",
     containerFactory = "userSurnameDtoKafkaListenerContainerFactory"
   )
   public void listen(ConsumerRecord<UUID, UserSurnameDto> response) {
      userDtoAggregatorService.enrichWith(response.key(), response.value());
   }
}

@Getter
class GetUserEvent extends ApplicationEvent {
   private final UUID uuid;
   private final Long id;

   public GetUserEvent(Object source, UUID uuid, Long id) {
      super(source);
      this.uuid = uuid;
      this.id = id;
   }
}

@Configuration
class KafkaConfig {
   @Bean
   public ProducerFactory<UUID, Long> producerFactory() {
      return new DefaultKafkaProducerFactory<>(Map.of(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, UUIDSerializer.class,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class
      ));
   }

   @Bean
   public KafkaTemplate<UUID, Long> kafkaTemplate() {
      return new KafkaTemplate<>(producerFactory());
   }

   @Bean
   public ConsumerFactory<UUID, UserNameDto> userNameDtoConsumerFactory() {
      Map<String, Object> props = commonConsumerProperties();
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserNameDtoDeserializer.class);
      return new DefaultKafkaConsumerFactory<>(props);
   }

   @Bean
   public ConcurrentKafkaListenerContainerFactory<UUID, UserNameDto> userNameDtoKafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<UUID, UserNameDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(userNameDtoConsumerFactory());
      return factory;
   }

   @Bean
   public ConsumerFactory<UUID, UserSurnameDto> userSurnameDtoConsumerFactory() {
      Map<String, Object> props = commonConsumerProperties();
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, UserSurnameDtoDeserializer.class);
      return new DefaultKafkaConsumerFactory<>(props);
   }

   @Bean
   public ConcurrentKafkaListenerContainerFactory<UUID, UserSurnameDto> userSurnameDtoKafkaListenerContainerFactory() {
      ConcurrentKafkaListenerContainerFactory<UUID, UserSurnameDto> factory = new ConcurrentKafkaListenerContainerFactory<>();
      factory.setConsumerFactory(userSurnameDtoConsumerFactory());
      return factory;
   }

   private Map<String, Object> commonConsumerProperties() {
      Map<String, Object> props = new HashMap<>();
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, UUIDDeserializer.class);
      return props;
   }
}
