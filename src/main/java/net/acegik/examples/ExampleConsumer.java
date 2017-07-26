package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.Map;
import net.acegik.jsondataflow.OpflowRpcHandler;
import net.acegik.jsondataflow.OpflowRpcListener;
import net.acegik.jsondataflow.OpflowRpcResponse;

public class ExampleConsumer {

    private static int fib(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] argv) throws Exception {
        final Gson gson = new Gson();
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

        final OpflowRpcHandler rpc = new OpflowRpcHandler(flowParams);
        rpc.process(new OpflowRpcListener() {
            @Override
            public void processMessage(byte[] content, Map<String, Object> info, OpflowRpcResponse response) throws IOException {
                String message = new String(content, "UTF-8");
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message);
                System.out.println(" [+] Received '" + jsonObject.toString() + "'");

                response.emitStarted();

                int number = Integer.parseInt(jsonObject.get("number").toString());
                FibonacciGenerator fibonacci = new FibonacciGenerator(number);

                while(fibonacci.next()) {
                    FibonacciGenerator.Result r = fibonacci.result();
                    response.emitProgress(r.getStep(), r.getNumber(), null);
                };

                String result = gson.toJson(fibonacci.result());
                System.out.println(" [-] Result '" + result + "'");

                response.emitCompleted(result);
            }
        });
    }
}
