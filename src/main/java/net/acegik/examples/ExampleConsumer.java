package net.acegik.examples;

import java.util.HashMap;
import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExampleConsumer {
    public static final JsonParser jsonParser = new JsonParser();
    public static void main(String[] argv) throws Exception {

        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("host", "192.168.56.56");
        flowParams.put("username", "master");
        flowParams.put("password", "zaq123edcx");
        flowParams.put("virtualHost", "/");
        flowParams.put("exchangeType", "direct");
        flowParams.put("exchangeName", "sample-exchange");
        flowParams.put("routingKey", "sample");
        flowParams.put("queueName", "sample-queue");

        final JsonObjectFlow flow = new JsonObjectFlow(flowParams);
        flow.addListener(new FlowChangeListener() {
            @Override
            public void objectReceived(FlowChangeEvent event) {
                String action = event.getAction();
                String message = event.getData().toString();
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message);
                if (Integer.parseInt(jsonObject.get("code").toString()) == 999) {
                    try {
                        flow.close();
                    } catch (Exception error) {
                        System.out.println("Error when close flow: " + error.getMessage());
                    }
                }
                System.out.println(" [x] Received '" + jsonObject.toString() + "'");
            }
        });
        flow.run();
    }
}
