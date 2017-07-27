package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import java.util.Map;
import net.acegik.jsondataflow.OpflowMessage;
import net.acegik.jsondataflow.OpflowRpcHandler;
import net.acegik.jsondataflow.OpflowRpcResult;

public class OpflowRpcExampleMaster {

    public static void main(String[] argv) throws Exception {
        final Gson gson = new Gson();
        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("uri", "amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000");
        flowParams.put("exchangeName", "tdd-opflow-exchange");
        flowParams.put("exchangeType", "direct");
        flowParams.put("routingKey", "sample");
        flowParams.put("operatorName", "tdd-opflow-queue");
        flowParams.put("responseName", "tdd-opflow-feedback");

        final OpflowRpcHandler rpc = new OpflowRpcHandler(flowParams);
        Map<String, Object> input = new HashMap<String, Object>();
        input.put("number", 20);
        OpflowRpcResult result = rpc.request(gson.toJson(input), flowParams);
        
        while(result.hasNext()) {
            OpflowMessage msg = result.next();
            System.out.println(" message: " + msg.getContentAsString());
        }
    }
}
