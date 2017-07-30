package net.acegik.examples;

import java.util.HashMap;
import com.google.gson.Gson;
import java.util.Map;
import net.acegik.jsondataflow.OpflowMessage;
import net.acegik.jsondataflow.OpflowRpcMaster;
import net.acegik.jsondataflow.OpflowRpcResult;
import net.acegik.jsondataflow.OpflowUtil;

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
        
        System.out.println("[+] ExampleMaster request");

        OpflowRpcResult result1 = rpc.request("fibonacci", OpflowUtil.buildJson(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("number", 20);
            }
        }), OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("timeout", 5);
                opts.put("mode", "standalone");
            }
        }));

        while(result1.hasNext()) {
            OpflowMessage msg = result1.next();
            System.out.println("[-] message1 received: " + msg.getContentAsString());
        }
        
        OpflowRpcResult result2 = rpc.request("fibonacci2", OpflowUtil.buildJson(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("number", 30);
            }
        }), OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("timeout", 30);
                //opts.put("mode", "standalone");
            }
        }));
        
        while(result2.hasNext()) {
            OpflowMessage msg = result2.next();
            System.out.println("[-] message2 received: " + msg.getContentAsString());
        }
        
//        try {
//            System.out.println("[-] sleep...");
//            Thread.sleep(2000);
//        } catch(InterruptedException e) {}
        
        System.out.println("[-] closing");
        rpc.close();
        
        System.out.println("[-] ExampleMaster has finished");
    }
}
