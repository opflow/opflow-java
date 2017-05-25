package net.acegik.examples;

import com.rabbitmq.client.*;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ExampleConsumer {
    public static void main(String[] argv) throws Exception {
        JsonObjectFlow flow = new JsonObjectFlow();
        flow.run();
    }
}
