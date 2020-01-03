package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowJsonTransformationException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowJsontool {
    private static final Gson GSON = new Gson();
    private static final Gson PSON = new GsonBuilder().setPrettyPrinting().create();
    
    public static String toString(Object jsonObj) {
        return toString(jsonObj, false);
    }
    
    public static String toString(Object jsonObj, boolean pretty) {
        return pretty ? PSON.toJson(jsonObj) : GSON.toJson(jsonObj);
    }
    
    public static String toString(Object[] objs, Type[] types) {
        return toString(objs, types, false);
    }
    
    public static String toString(Object[] objs, Type[] types, boolean pretty) {
        JsonArray array = new JsonArray();
        for(int i=0; i<objs.length; i++) {
            array.add(GSON.toJson(objs[i], types[i]));
        }
        return pretty ? PSON.toJson(array) : GSON.toJson(array);
    }
    
    public static String toString(Map<String, Object> jsonMap) {
        return toString(jsonMap, false);
    }
    
    public static String toString(Map<String, Object> jsonMap, boolean pretty) {
        return pretty ? PSON.toJson(jsonMap) : GSON.toJson(jsonMap);
    }
    
    public static <T> T toObject(String json, Class<T> type) {
        return GSON.fromJson(json, type);
    }
    
    public static <T> T toObject(InputStream inputStream, Class<T> type) {
        try {
            return toObject(new InputStreamReader(inputStream, "UTF-8"), type);
        } catch (UnsupportedEncodingException exception) {
            throw new OpflowJsonTransformationException(exception);
        }
    }
    
    public static <T> T toObject(Reader reader, Class<T> type) {
        return GSON.fromJson(new JsonReader(reader), type);
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
        JsonArray array = JsonParser.parseString(arrayString).getAsJsonArray();
        Object[] args = new Object[types.length];
        for(int i=0; i<types.length; i++) {
            args[i] = GSON.fromJson(array.get(i), types[i]);
        }
        return args;
    }
    
    public static <T> T extractField(String json, String fieldName, Class<T> type) {
        try {
            JsonObject jsonObject = (JsonObject)JsonParser.parseString(json);
            return type.cast(jsonObject.get(fieldName));
        } catch (ClassCastException | JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static int extractFieldAsInt(String json, String fieldName) {
        try {
            JsonObject jsonObject = (JsonObject)JsonParser.parseString(json);
            return jsonObject.get(fieldName).getAsInt();
        } catch (JsonSyntaxException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
}
