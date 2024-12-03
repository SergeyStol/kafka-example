package dev.sstol.srv1;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * @author Sergey Stol
 * 2024-12-03
 */
public class UserSurnameDtoDeserializer implements Deserializer<UserSurnameDto> {

   private final ObjectMapper objectMapper = new ObjectMapper();

   @Override
   public UserSurnameDto deserialize(String topic, byte[] data) {
      try {
         return objectMapper.readValue(data, UserSurnameDto.class);
      } catch (Exception e) {
         throw new RuntimeException("Error deserializing UserSurnameDto", e);
      }
   }
}