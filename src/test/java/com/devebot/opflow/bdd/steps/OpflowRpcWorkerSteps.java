package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowRpcWorker;
import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.lab.FibonacciGenerator;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.jbehave.core.annotations.Given;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */

public class OpflowRpcWorkerSteps {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWorkerSteps.class);
    
    private final Map<String, OpflowRpcWorker> workers = new HashMap<String, OpflowRpcWorker>();
    private final Gson gson = new Gson();
    private final JsonParser jsonParser = new JsonParser();

    @Given("a RPC worker<$string>")
    public void createRpcWorker(String workerName) throws OpflowConstructorException {
        workers.put(workerName, OpflowHelper.createRpcWorker());
    }
    
    @Given("a counter consumer in worker<$string>")
    public void consumeAllMessage(String workerName) {
        workers.get(workerName).process(new OpflowRpcListener() {
            @Override
            public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                if (LOG.isTraceEnabled()) LOG.trace("[+] Routine input: " + message.getContentAsString());
                return OpflowRpcListener.NEXT;
            }
        });
    }
    
    @Given("a FibonacciGenerator consumer with names '$string' in worker<$string>")
    public void consumeFibonacciGenerator(String names, String workerName) {
        workers.get(workerName).process(new String[] {"fibonacci", "fib"}, new OpflowRpcListener() {
            @Override
            public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                JsonObject jsonObject = (JsonObject)jsonParser.parse(message.getContentAsString());
                if (LOG.isTraceEnabled()) LOG.trace("[+] Fibonacci received: '" + jsonObject.toString() + "'");

                response.emitStarted();

                int number = Integer.parseInt(jsonObject.get("number").toString());
                FibonacciGenerator fibonacci = new FibonacciGenerator(number);

                while(fibonacci.next()) {
                    FibonacciGenerator.Result r = fibonacci.result();
                    response.emitProgress(r.getStep(), r.getNumber(), null);
                }

                String result = gson.toJson(fibonacci.result());
                if (LOG.isTraceEnabled()) LOG.trace("[-] Fibonacci finished with: '" + result + "'");

                response.emitCompleted(result);
                
                return null;
            }
        });
    }
}
