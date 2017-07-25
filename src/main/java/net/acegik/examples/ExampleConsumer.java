package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.acegik.jsondataflow.OpflowChangeEvent;
import net.acegik.jsondataflow.OpflowChangeFeedback;
import net.acegik.jsondataflow.OpflowChangeListener;
import net.acegik.jsondataflow.OpflowRPC;

public class ExampleConsumer {

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] argv) throws Exception {
        final JsonParser jsonParser = new JsonParser();
        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("host", "192.168.56.56");
        flowParams.put("username", "master");
        flowParams.put("password", "zaq123edcx");
        flowParams.put("virtualHost", "/");
        flowParams.put("exchangeType", "direct");
        flowParams.put("exchangeName", "tdd-opflow-exchange");
        flowParams.put("routingKey", "sample");
        flowParams.put("queueName", "tdd-opflow-queue");
        flowParams.put("feedback.queueName", "tdd-opflow-feedback");

        final OpflowRPC flow = new OpflowRPC(flowParams);
        flow.process(new OpflowChangeListener() {
            @Override
            public void objectReceived(OpflowChangeEvent event, OpflowChangeFeedback feedback) {
                String message = event.getData().toString();
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message);
                System.out.println(" [x] Received '" + jsonObject.toString() + "'");
                
                feedback.emitStarted();
                
                int number = Integer.parseInt(jsonObject.get("number").toString());
                String result = (""+fib(number));
                System.out.println(" [x] Result '" + result + "'");
                
                feedback.emitCompleted(result);
            }
        });
    }
}
