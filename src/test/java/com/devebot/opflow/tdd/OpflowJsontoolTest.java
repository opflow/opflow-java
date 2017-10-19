package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.lab.FibonacciPacket;
import com.devebot.opflow.lab.FibonacciPacketList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 * @author drupalex
 */
public class OpflowJsontoolTest {
    
    @Test
    public void testFromJson() {
        Map<String, Object> opts = new HashMap<String, Object>();
        opts.put("string", "Hello world");
        opts.put("integer", 177);
        opts.put("double", 19.79);
        opts.put("boolean", true);
        opts.put("nullable", null);
        String json = OpflowJsontool.toString(opts);
        
        System.out.println("Json string: " + json);
        
        Map<String, Object> jsonObj = OpflowJsontool.toObjectMap(json);
        
        for(String fieldName: jsonObj.keySet()) {
            System.out.println(MessageFormat.format("Field [{0}] has value: {1}", new Object[] {
                fieldName, jsonObj.get(fieldName)
            }));
        }
    }
    
    private static final Gson GSON1;
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Object.class, new JsonDeserializer<Object>() {
            @Override
            public Object deserialize(JsonElement json, Type typeOfT,
                    JsonDeserializationContext context) {
                System.out.println("JsonElement: " + json.toString());
                if (json.isJsonNull()) {
                    return null;
                } else if (json.isJsonPrimitive()) {
                    return handlePrimitive(json.getAsJsonPrimitive());
                } else if (json.isJsonArray()) {
                    return handleArray(json.getAsJsonArray(), context);
                } else {
                    return handleObject(json.getAsJsonObject(), context);
                }
            }

            private Object handlePrimitive(JsonPrimitive json) {
                if (json.isBoolean()) {
                    return json.getAsBoolean();
                } else if (json.isString()) {
                    return json.getAsString();
                } else {
                    BigDecimal bigDec = json.getAsBigDecimal();
                    // Find out if it is an int type
                    try {
                        bigDec.toBigIntegerExact();
                        try {
                            return bigDec.intValueExact();
                        } catch (ArithmeticException e) {
                        }
                        return bigDec.longValue();
                    } catch (ArithmeticException e) {
                    }
                    // Just return it as a double
                    return bigDec.doubleValue();
                }
            }

            private Object handleArray(JsonArray json, JsonDeserializationContext context) {
                Object[] array = new Object[json.size()];
                for (int i = 0; i < array.length; i++) {
                    array[i] = context.deserialize(json.get(i), Object.class);
                }
                return array;
            }

            private Object handleObject(JsonObject json, JsonDeserializationContext context) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (Map.Entry<String, JsonElement> entry : json.entrySet()) {
                    map.put(entry.getKey(), context.deserialize(entry.getValue(), Object.class));
                }
                return map;
            }
        });
        GSON1 = gsonBuilder.create();
    }
    
    @Test
    public void test_JsonDeserializer() {
        Map<String, Object> opts = new HashMap<String, Object>();
        opts.put("string", "Hello world");
        opts.put("integer", 177);
        opts.put("double", 19.79);
        opts.put("boolean", true);
        opts.put("nullable", null);
        String json = OpflowJsontool.toString(opts);
        
        System.out.println("Json string: " + json);
        
        Map<String,Object> jsonObj = GSON1.fromJson(json, Map.class);

        System.out.println("Integer type: " + jsonObj.get("integer").getClass());
    }
    
    
    private static final Gson GSON2;
    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(Double.class,  new JsonSerializer<Double>() {
            @Override
            public JsonElement serialize(Double src, Type typeOfSrc, JsonSerializationContext context) {
                if (src == src.intValue()) return new JsonPrimitive(src.intValue());
                if (src == src.longValue()) return new JsonPrimitive(src.longValue());
                return new JsonPrimitive(src.intValue());
            }
        });
        GSON2 = gsonBuilder.create();
    }
    
    @Test
    public void testSingleObjectContainsGeneric() {
        FibonacciPacketList packets = new FibonacciPacketList();
        packets.add(new FibonacciPacket(100));
        packets.add(new FibonacciPacket(200));
        String json = OpflowJsontool.toString(packets);
        System.out.println("Json string: " + json);

        FibonacciPacketList packet2 = OpflowJsontool.toObject(json, FibonacciPacketList.class);
        
        for(FibonacciPacket packet: packet2.getList()) {
            System.out.println(MessageFormat.format("Number: {0}", new Object[] {
                packet.getNumber()
            }));
        }
    }
    
    @Test
    public void testArrayOfObjectsContainsGeneric() {
        FibonacciPacketList packets = new FibonacciPacketList();
        packets.add(new FibonacciPacket(100));
        packets.add(new FibonacciPacket(200));
        
        Object[] args = new Object[] { packets, "Hello world", 1024 };
        
        String json = OpflowJsontool.toString(args);
        System.out.println("Json string: " + json);

        Object[] arg2 = OpflowJsontool.toObjectArray(json, new Class[] {
            FibonacciPacketList.class, String.class, Integer.class
        });
        FibonacciPacketList packet2 = (FibonacciPacketList) arg2[0];
        
        for(FibonacciPacket packet: packet2.getList()) {
            System.out.println(MessageFormat.format("Number: {0}", new Object[] {
                packet.getNumber()
            }));
        }
    }
}
