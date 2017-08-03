package com.devebot.opflow.bdd.steps;

/**
 *
 * @author drupalex
 */
import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowRpcMaster;
import com.devebot.opflow.OpflowRpcRequest;
import com.devebot.opflow.OpflowRpcResult;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.lab.FibonacciGenerator;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.Map;

import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OpflowRpcMasterSteps {
    private final JsonParser jsonParser = new JsonParser();
    private final Map<String, OpflowRpcMaster> masters =  new HashMap<String, OpflowRpcMaster>();
    private final Map<String, OpflowRpcRequest> requests = new HashMap<String, OpflowRpcRequest>();
    private final Map<String, Integer> inputs = new HashMap<String, Integer>();
    
    @Given("a RPC master<$masterName>")
    public void createRpcMaster(String masterName) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster());
    }
    
    @Given("a RPC master<$masterName> with properties file: '$propFile'")
    public void createRpcMaster(String masterName, String propFile) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster(propFile));
    }
    
    @When("I make a request<$requestName> to routine<$routineId> in master<$masterName> with input number: $number")
    public void makeRequest(final String requestName, final String routineId, final String masterName, final int number) {
        inputs.put(requestName, number);
        OpflowRpcRequest request = masters.get(masterName).request("fibonacci", OpflowUtil.buildJson(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("number", number);
            }
        }), OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("timeout", 5);
            }
        }));
        requests.put(requestName, request);
    }

    @Then("the request<$requestName> should finished successfully")
    public void checkRequestOutput(String requestName) {
        OpflowRpcResult output = requests.get(requestName).exhaust();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(output.getValueAsString());
        
        int number = Integer.parseInt(jsonObject.get("number").toString());
        assertThat(number, equalTo(inputs.get(requestName)));
        
        FibonacciGenerator fibGen = new FibonacciGenerator(number);
        FibonacciGenerator.Result fibResult = fibGen.finish();
        
        int step = Integer.parseInt(jsonObject.get("step").toString());
        assertThat(step, equalTo(fibResult.getStep()));
        assertThat(step, equalTo(output.getProgress().length));
        
        long value = Long.parseLong(jsonObject.get("value").toString());
        assertThat(value, equalTo(fibResult.getValue()));
    }
}