package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.Map;
import net.acegik.jsondataflow.OpflowMessage;
import net.acegik.jsondataflow.OpflowRpcHandler;
import net.acegik.jsondataflow.OpflowRpcListener;
import net.acegik.jsondataflow.OpflowRpcResponse;
import net.acegik.jsondataflow.OpflowRpcResult;

public class OpflowRpcExample {

    public static void main(String[] argv) throws Exception {
        final Gson gson = new Gson();
        final JsonParser jsonParser = new JsonParser();
        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("uri", "amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000");
        flowParams.put("exchangeName", "tdd-opflow-exchange");
        flowParams.put("exchangeType", "direct");
        flowParams.put("routingKey", "sample");
        flowParams.put("operatorName", "tdd-opflow-queue");
        flowParams.put("responseName", "tdd-opflow-feedback");

        final OpflowRpcHandler rpc = new OpflowRpcHandler(flowParams);
        rpc.process(new OpflowRpcListener() {
            @Override
            public void processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                JsonObject jsonObject = (JsonObject)jsonParser.parse(new String(message.getContent(), "UTF-8"));
                System.out.println(" [+] Received '" + jsonObject.toString() + "'");

                response.emitStarted();

                int number = Integer.parseInt(jsonObject.get("number").toString());
                FibonacciGenerator fibonacci = new FibonacciGenerator(number);

                while(fibonacci.next()) {
                    FibonacciGenerator.Result r = fibonacci.result();
                    response.emitProgress(r.getStep(), r.getNumber(), null);
                }

                String result = gson.toJson(fibonacci.result());
                System.out.println(" [-] Result '" + result + "'");

                response.emitCompleted(result);
            }
        });
        
        Map<String, Object> input = new HashMap<String, Object>();
        input.put("number", 20);
        OpflowRpcResult result = rpc.request(gson.toJson(input), flowParams);
        
        while(result.hasNext()) {
            OpflowMessage msg = result.next();
            System.out.println(" message: " + msg.getContentAsString());
        }
    }
}
