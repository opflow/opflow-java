package net.acegik.examples;

import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExampleConsumer {
    public static final JsonParser jsonParser = new JsonParser();
    public static void main(String[] argv) throws Exception {
        JsonObjectFlow flow = new JsonObjectFlow();
        flow.addListener(new FlowChangeListener() {
            @Override
            public void objectReceived(FlowChangeEvent event) {
                String action = event.getAction();
                String message = event.getData().toString();
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message);
                System.out.println(" [x] Received '" + jsonObject.toString() + "'");
            }
        });
        flow.run();
    }
}
