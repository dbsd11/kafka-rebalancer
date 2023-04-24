package group.bison.kafka.rebalancer.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.util.StringUtils;

import java.io.IOException;

/**
 * <Description>
 *
 * @author yuan
 * @since 2019-03-03
 */
public class JsonUtil {
    static final ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    public static String toJson(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj.getClass() == String.class) {
            return obj.toString();
        }
        
        String json;
        try {
            json = mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return json;
    }


    public static <T> T fromJson(String content, Class<T> classType) {
        if (StringUtils.isEmpty(content)) {
            return null;
        }
        if (classType == String.class) {
            return (T) content;
        }
        try {
            return mapper.readValue(content, classType);
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

}
