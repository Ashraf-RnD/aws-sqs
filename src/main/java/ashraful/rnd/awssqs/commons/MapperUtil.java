package ashraful.rnd.awssqs.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@AllArgsConstructor
public class MapperUtil {
    private final ObjectMapper objectMapper;

    public String objectToJson(Object data) {
        try {

            return objectMapper.writeValueAsString(data);
        } catch (JsonProcessingException ex) {
            log.error("Could not convert object to string {}", data, ex);

            return null;
        }
    }

    public <T> T jsonToObject(Object data, Class<T> tClass) {
        try {

            return objectMapper.readValue(String.valueOf(data), tClass);
        } catch (Exception ex) {
            log.error("Could not convert object {} to class {}", data, tClass, ex);

            return null;
        }
    }

    public <T> T convertToType(Object data, Class<T> tClass) {
        try {

            return jsonToObject(objectToJson(data), tClass);
        } catch (Exception ex) {
            log.error("Could not convert object {} to class {}", data, tClass, ex);

            return null;
        }
    }
}
