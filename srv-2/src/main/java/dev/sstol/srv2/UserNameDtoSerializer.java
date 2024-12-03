package dev.sstol.srv2;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * @author Sergey Stol
 * 2024-12-03
 */
public class UserNameDtoSerializer implements Serializer<UserNameDto> {
   private final ObjectMapper objectMapper = new ObjectMapper();
   @Override
   public byte[] serialize(String topic, UserNameDto userNameDto) {
      try {
         return userNameDto != null ? objectMapper.writeValueAsString(userNameDto).getBytes(StandardCharsets.UTF_8) : null;
      } catch (JsonProcessingException e) {
         throw new RuntimeException(e);
      }
   }
}