package com.devebot.opflow.supports;

import com.devebot.opflow.annotation.OpflowFieldExclude;
import com.devebot.opflow.exception.OpflowJsonSyntaxException;
import com.devebot.opflow.exception.OpflowJsonTransformationException;
import com.google.gson.nostro.ExclusionStrategy;
import com.google.gson.nostro.FieldAttributes;
import com.google.gson.nostro.Gson;
import com.google.gson.nostro.GsonBuilder;
import com.google.gson.nostro.JsonArray;
import com.google.gson.nostro.JsonDeserializationContext;
import com.google.gson.nostro.JsonDeserializer;
import com.google.gson.nostro.JsonElement;
import com.google.gson.nostro.JsonObject;
import com.google.gson.nostro.JsonParseException;
import com.google.gson.nostro.JsonParser;
import com.google.gson.nostro.JsonPrimitive;
import com.google.gson.nostro.JsonSerializationContext;
import com.google.gson.nostro.JsonSerializer;
import com.google.gson.nostro.JsonSyntaxException;
import com.google.gson.nostro.stream.JsonReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

/**
 *
 * @author drupalex
 */
public class OpflowJsonTool {
    private static final ExclusionStrategy STRATEGY = new ExclusionStrategy() {
        @Override
        public boolean shouldSkipClass(Class<?> clazz) {
            return false;
        }

        @Override
        public boolean shouldSkipField(FieldAttributes field) {
            return field.getAnnotation(OpflowFieldExclude.class) != null;
        }
    };

    private static final GsonUTCDateAdapter GSON_UTC_DATE_ADAPTER = new GsonUTCDateAdapter();
    private static final Gson GSON = new GsonBuilder()
            .addSerializationExclusionStrategy(STRATEGY)
            .registerTypeAdapter(Date.class, GSON_UTC_DATE_ADAPTER)
            .create();
    private static final Gson PSON = new GsonBuilder()
            .addSerializationExclusionStrategy(STRATEGY)
            .registerTypeAdapter(Date.class, GSON_UTC_DATE_ADAPTER)
            .setPrettyPrinting()
            .create();
    
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
        try {
            return GSON.fromJson(json, type);
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
    }
    
    public static <T> T toObject(InputStream inputStream, Class<T> type) {
        try {
            return toObject(new InputStreamReader(inputStream, "UTF-8"), type);
        }
        catch (UnsupportedEncodingException exception) {
            throw new OpflowJsonTransformationException(exception);
        }
    }
    
    public static <T> T toObject(Reader reader, Class<T> type) {
        try {
            return GSON.fromJson(new JsonReader(reader), type);
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
    }
    
    public static Map<String, Object> toObjectMap(String json) {
        try {
            Map<String,Object> map = GSON.fromJson(json, Map.class);
            return map;
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
    }
    
    public static Map<String, Object> toObjectMap(InputStream inputStream) {
        try {
            Map<String,Object> map = GSON.fromJson(new InputStreamReader(inputStream, "UTF-8"), Map.class);
            return map;
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
        catch (UnsupportedEncodingException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static Object[] toObjectArray(String arrayString, Type[] types) {
        try {
            if (arrayString == null) return new Object[0];
            JsonArray array = JsonParser.parseString(arrayString).getAsJsonArray();
            Object[] args = new Object[types.length];
            for(int i=0; i<types.length; i++) {
                args[i] = GSON.fromJson(array.get(i), types[i]);
            }
            return args;
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
    }
    
    public static <T> T extractField(String json, String fieldName, Class<T> type) {
        try {
            JsonObject jsonObject = (JsonObject)JsonParser.parseString(json);
            return type.cast(jsonObject.get(fieldName));
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
        catch (ClassCastException e) {
            throw new OpflowJsonTransformationException(e);
        }
    }
    
    public static int extractFieldAsInt(String json, String fieldName) {
        try {
            JsonObject jsonObject = (JsonObject)JsonParser.parseString(json);
            return jsonObject.get(fieldName).getAsInt();
        }
        catch (JsonSyntaxException e) {
            throw new OpflowJsonSyntaxException(e);
        }
    }
    
    private static class GsonUTCDateAdapter implements JsonSerializer<Date>, JsonDeserializer<Date> {
        private final DateFormat dateFormat;

        public GsonUTCDateAdapter() {
            dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        }

        @Override
        public synchronized JsonElement serialize(Date date, Type type, JsonSerializationContext jsonSerializationContext) {
            return new JsonPrimitive(dateFormat.format(date));
        }

        @Override
        public synchronized Date deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) {
            try {
                return dateFormat.parse(jsonElement.getAsString());
            } catch (ParseException e) {
                throw new JsonParseException(e);
            }
        }
    }
}
