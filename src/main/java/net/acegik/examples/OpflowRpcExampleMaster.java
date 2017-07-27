package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import java.util.Map;
import net.acegik.jsondataflow.OpflowMessage;
import net.acegik.jsondataflow.OpflowRpcMaster;
import net.acegik.jsondataflow.OpflowRpcResult;

public class OpflowRpcExampleMaster {

    public static void main(String[] argv) throws Exception {
        final Gson gson = new Gson();
        final HashMap<String, Object> flowParams = new HashMap<String, Object>();
        flowParams.put("uri", "amqp://master:zaq123edcx@192.168.56.56?frameMax=0x1000");
        flowParams.put("exchangeName", "tdd-opflow-exchange");
        flowParams.put("routingKey", "sample");
        flowParams.put("operatorName", "tdd-opflow-queue");
        flowParams.put("responseName", "tdd-opflow-feedback");

        final OpflowRpcMaster rpc = new OpflowRpcMaster(flowParams);
        
        Map<String, Object> input = new HashMap<String, Object>();
        input.put("number", 20);
        String inputStr = gson.toJson(input);
        
        System.out.println("[+] ExampleMaster request: " + inputStr);
        Map<String, Object> opts = new HashMap<String, Object>();
        opts.put("routineId", "fibonacci");
        opts.put("timeout", 5);
        OpflowRpcResult result = rpc.request(inputStr, opts);
        
        input.put("number", 30);
        opts.put("timeout", 30);
        OpflowRpcResult result2 = rpc.request(gson.toJson(input), opts);
        
        while(result.hasNext()) {
            OpflowMessage msg = result.next();
            System.out.println("[-] message received: " + msg.getContentAsString());
        }
        
        System.out.println("[-] ExampleMaster has finished");
    }
}
