package dev.sstol.srv3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * @author Sergey Stol
 * 2024-12-03
 */
public class UserSurnameDtoSerializer implements Serializer<UserSurnameDto> {
   private final ObjectMapper objectMapper = new ObjectMapper();
   @Override
   public byte[] serialize(String topic, UserSurnameDto userSurnameDto) {
      try {
         return userSurnameDto != null ? objectMapper.writeValueAsString(userSurnameDto).getBytes(StandardCharsets.UTF_8) : null;
      } catch (JsonProcessingException e) {
         throw new RuntimeException(e);
      }
   }
}