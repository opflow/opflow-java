package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowJsonTransformationException;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowJsontool {
    private static final Logger LOG = LoggerFactory.getLogger(OpflowJsontool.class);
    private final OpflowLogTracer logTracer = OpflowLogTracer.ROOT.copy();
    
    private static final Gson GSON = new Gson();
    private static final JsonParser JSON_PARSER = new JsonParser();
    
    public static String toString(Object jsonObj) {
        return GSON.toJson(jsonObj);
    }
    
    public static String toString(Map<String, Object> jsonMap) {
        return GSON.toJson(jsonMap);
    }
    
    public static <T> T toObject(String json, Class<T> type) {
        return GSON.fromJson(json, type);
    }
    
    public static <T> T toObject(OpflowMessage message, Class<T> type) {
        return GSON.fromJson(message.getBodyAsString(), type);
    }
    
    public static Map<String, Object> toObjectMap(String json) {
        try {
            Map<String,Object> map = GSON.fromJson(json, Map.class);
            return map;
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static Object[] toObjectArray(String arrayString, Class[] types) {
        if (arrayString == null) return new Object[0];
        JsonArray array = JSON_PARSER.parse(arrayString).getAsJsonArray();
        Object[] args = new Object[types.length];
        for(int i=0; i<types.length; i++) {
            args[i] = GSON.fromJson(array.get(i), types[i]);
        }
        return args;
    }
    
    public static <T> T extractField(String json, String fieldName, Class<T> type) {
        try {
            JsonObject jsonObject = (JsonObject)JSON_PARSER.parse(json);
            return type.cast(jsonObject.get(fieldName));
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static int extractFieldAsInt(String json, String fieldName) {
        try {
            JsonObject jsonObject = (JsonObject)JSON_PARSER.parse(json);
            return jsonObject.get(fieldName).getAsInt();
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
}
