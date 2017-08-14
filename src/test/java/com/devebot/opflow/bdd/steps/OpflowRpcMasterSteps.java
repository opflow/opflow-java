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
import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItemInArray;
import org.jbehave.core.annotations.Named;

public class OpflowRpcMasterSteps {
    private final JsonParser jsonParser = new JsonParser();
    private final Map<String, OpflowRpcMaster> masters =  new HashMap<String, OpflowRpcMaster>();
    private final Map<String, OpflowRpcRequest> requests = new HashMap<String, OpflowRpcRequest>();
    private final Map<String, Integer> inputs = new HashMap<String, Integer>();
    
    @Given("a RPC master<$masterName>")
    public void createRpcMaster(@Named("masterName") final String masterName) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster());
    }
    
    @Given("a RPC master<$masterName> with properties file: '$propFile'")
    public void createRpcMaster(@Named("masterName") final String masterName, 
            @Named("propFile") final String propFile) throws OpflowConstructorException {
        masters.put(masterName, OpflowHelper.createRpcMaster(propFile));
    }
    
    @When("I make a request<$requestName> to routine<$routineId> in master<$masterName> with input number: $number")
    public void makeRequest(@Named("requestName") final String requestName, 
            @Named("routineId") final String routineId, 
            @Named("masterName") final String masterName, 
            @Named("number") final int number) {
        makeRequest(requestName, routineId, masterName, number, 5000);
    }

    @When("I make a request<$requestName>($number) to routine<$routineId> in master<$masterName> with timeout: $timeout")
    public void makeRequest(@Named("requestName") final String requestName, 
            @Named("routineId") final String routineId, 
            @Named("masterName") final String masterName, 
            @Named("number") final int number, 
            @Named("timeout") final long timeout) {
        inputs.put(requestName, number);
        OpflowRpcRequest request = masters.get(masterName).request("fibonacci", OpflowUtil.buildJson(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("number", number);
            }
        }), OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("timeout", timeout);
            }
        }));
        requests.put(requestName, request);
    }
    
    @Then("the request<$requestName> should finished successfully")
    public void checkRequestOutput(@Named("requestName") final String requestName) {
        OpflowRpcResult output = OpflowUtil.exhaustRequest(requests.get(requestName));
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
    
    @When("I make requests from number $fromNumber to number $toNumber to routine<$routineId> in master<$masterName>")
    public void makeRangeOfRequests(@Named("fromNumber") final int fromNumber, 
            @Named("toNumber") final int toNumber, 
            @Named("routineId") final String routineId, 
            @Named("masterName") final String masterName) {
        for(int number = fromNumber; number < toNumber; number++) {
            String requestName = "reqseq" + number;
            makeRequest(requestName, routineId, masterName, number);
        }
    }
    
    @Then("the requests from $fromNumber to $toNumber should finished successfully")
    public void checkRangeOfRequests(@Named("fromNumber") final int fromNumber, 
            @Named("toNumber") final int toNumber) {
        for(int i = fromNumber; i < toNumber; i++) {
            String requestName = "reqseq" + i;
            checkRequestOutput(requestName);
        }
    }
    
    @When("I close RPC master<$masterName>")
    public void closeRpcMaster(@Named("masterName") String masterName) {
        masters.get(masterName).close();
    }
    
    @Then("the RPC master<$masterName> connection is '$status'")
    public void checkRpcMaster(@Named("masterName") String masterName, @Named("status") String status) {
        OpflowRpcMaster.State state = masters.get(masterName).check();
        List<String> collection = Lists.newArrayList("opened", "closed");
        assertThat(collection, hasItem(status));
        if ("opened".equals(status)) {
            assertThat(OpflowRpcMaster.State.CONNECTION_OPENED, equalTo(state.getConnectionState()));
        } else if ("closed".equals(status)) {
            assertThat(OpflowRpcMaster.State.CONNECTION_CLOSED, equalTo(state.getConnectionState()));
        }
    }
}