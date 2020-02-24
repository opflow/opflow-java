package com.devebot.opflow.supports;

import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.exception.OpflowJsonTransformationException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
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
    private static final GsonUTCDateAdapter GSON_UTC_DATE_ADAPTER = new GsonUTCDateAdapter();
    private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Date.class, GSON_UTC_DATE_ADAPTER).create();
    private static final Gson PSON = new GsonBuilder().registerTypeAdapter(Date.class, GSON_UTC_DATE_ADAPTER).setPrettyPrinting().create();
    
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
    
    public static Map<String, Object> toObjectMap(InputStream inputStream) {
        try {
            Map<String,Object> map = GSON.fromJson(new InputStreamReader(inputStream, "UTF-8"), Map.class);
            return map;
        } catch (JsonSyntaxException | UnsupportedEncodingException e) {
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
