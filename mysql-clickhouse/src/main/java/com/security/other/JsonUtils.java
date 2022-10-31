package com.security.other;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Map;

import static java.math.BigDecimal.ROUND_UNNECESSARY;

@Slf4j
public class JsonUtils {

    @Getter
    private static ObjectMapper objectMapper = new ObjectMapper();
    //private static XmlMapper xmlMapper = new XmlMapper();
    private static String dateFormat;

    static {
        dateFormat = "yyyy-MM-dd HH:mm:ss";
        objectMapper.getSerializationConfig().with(new SimpleDateFormat(dateFormat));
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.registerModules(new SimpleModule().addSerializer(BigDecimal.class, new JsonSerializer<BigDecimal>() {
            @Override
            public void serialize(BigDecimal value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                gen.writeNumber(value.setScale(2, ROUND_UNNECESSARY));
            }
        }), new SimpleModule().addDeserializer(BigDecimal.class, new JsonDeserializer<BigDecimal>() {
            @Override
            public BigDecimal deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                return p.getDecimalValue().setScale(2, ROUND_UNNECESSARY);
            }
        }));

    }

    /**
     * 将对象转换为json
     *
     * @param obj
     * @return
     * @throws Exception
     */
    public static String convertObjectToJson(Object obj) throws RuntimeException {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }


    /**
     * 将json转换为对象
     *
     * @param json
     * @param classOfT
     * @param <T>
     * @return
     * @throws Exception
     */
    public static <T> T convertJsonToObject(String json, Class<T> classOfT) throws RuntimeException {
        try {
            return objectMapper.readValue(json, classOfT);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * 将json转换为对象
     *
     * @param json
     * @param valueTypeRef
     * @param <T>
     * @return
     */
    public static <T> T convertJsonToObject(String json, TypeReference<T> valueTypeRef) throws Exception {
        return objectMapper.readValue(json, valueTypeRef);
    }

    /**
     * 将inputStream转换为对象
     *
     * @param inputStream
     * @param valueTypeRef
     * @param <T>
     * @return
     */
    public static <T> T convertJsonToObject(InputStream inputStream, TypeReference<T> valueTypeRef) throws Exception {
        return objectMapper.readValue(inputStream, valueTypeRef);
    }

    /**
     * 将json转换为对象
     *
     * @param json
     * @return
     * @throws Exception
     */
    public static Map<String, Object> convertJsonToObject(String json) throws RuntimeException {
        try {
            return convertJsonToObject(json, Map.class);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * 将json转换成JsonNode
     *
     * @param json
     * @return
     */
    public static JsonNode convertJsonToJsonNode(String json) throws Exception {
        return objectMapper.readTree(json);
    }

    /**
     * 将对象转换成JsonNode
     *
     * @param obj
     * @return
     */
    public static <T extends JsonNode> T convertObjectToJsonNode(Object obj) throws RuntimeException {
        try {
            return objectMapper.valueToTree(obj);
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    /**
     * 将JsonNode转换成Map
     *
     * @param jsonNode
     * @return
     * @throws Exception
     */
    public static Map convertJsonNodeToMap(JsonNode jsonNode) throws Exception {
        return objectMapper.treeToValue(jsonNode, Map.class);
    }

    /**
     * 将JsonNode转换成指定类型的对象
     *
     * @param jsonNode
     * @return
     * @throws Exception
     */
    public static <T> T convertJsonNodeToObject(JsonNode jsonNode, Class<T> classOfT) throws Exception {
        return objectMapper.treeToValue(jsonNode, classOfT);
    }

    /**
     * 将对象转换成Map
     *
     * @param obj
     * @return
     * @throws Exception
     */
    public static Map convertObjectToMap(Object obj) throws Exception {
        return objectMapper.treeToValue(convertObjectToJsonNode(obj), Map.class);
    }

    /**
     * 将对象转换成指定类型的对象
     *
     * @param obj
     * @return
     * @throws Exception
     */
    public static <T> T convertObjectToObject(Object obj, Class<T> classOfT) throws Exception {
        return objectMapper.treeToValue(convertObjectToJsonNode(obj), classOfT);
    }

    /**
     * 创建ObjectNode
     *
     * @return
     * @throws Exception
     */
    public static ObjectNode createObjectNode() throws Exception {
        return objectMapper.createObjectNode();
    }

    /**
     * 创建ArrayNode
     *
     * @return
     * @throws Exception
     */
    public static ArrayNode createArrayNode() throws Exception {
        return objectMapper.createArrayNode();
    }

    /**
     * json格式化
     *
     * @return
     * @throws Exception
     */
    public static String jsonFormatter(Object obj) throws Exception {
        return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }


}
