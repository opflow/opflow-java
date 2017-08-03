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
    
    @Given("a RPC master[$string]")
    public void createRpcMaster(String masterName) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster());
    }
    
    @Given("a RPC master[$string] with properties file: '$string'")
    public void createRpcMaster(String masterName, String properties) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster(properties));
    }

    private int inputNumber;
    
    @When("I make a request[$string] to routine[$string] in master[$string] with input number: $number")
    public void makeRequest(final String requestName, final String routineId, final String masterName, final int number) {
        inputNumber = number;
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

    @Then("the request[$string] should finished successfully")
    public void checkRequestOutput(String requestName) {
        OpflowRpcResult output = requests.get(requestName).exhaust();
        JsonObject jsonObject = (JsonObject)jsonParser.parse(output.getValueAsString());
        int number = Integer.parseInt(jsonObject.get("number").toString());
        assertThat(number, equalTo(inputNumber));

        FibonacciGenerator fibGen = new FibonacciGenerator(number);
        FibonacciGenerator.Result fibResult = fibGen.finish();
        
        int step = Integer.parseInt(jsonObject.get("step").toString());
        assertThat(step, equalTo(fibResult.getStep()));
        assertThat(step, equalTo(output.getProgress().length));
        
        long value = Long.parseLong(jsonObject.get("value").toString());
        assertThat(value, equalTo(fibResult.getValue()));
    }

}