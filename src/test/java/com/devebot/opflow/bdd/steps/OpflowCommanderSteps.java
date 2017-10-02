package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowCommander;
import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.lab.FibonacciCalculator;
import com.devebot.opflow.lab.FibonacciPacket;
import com.devebot.opflow.lab.FibonacciResult;
import com.devebot.opflow.lab.FibonacciUtil;
import java.util.HashMap;
import java.util.Map;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 *
 * @author drupalex
 */
public class OpflowCommanderSteps {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommanderSteps.class);
    private final Map<String, OpflowCommander> commanders = new HashMap<String, OpflowCommander>();
    private final Map<String, Integer> requestInfos = new HashMap<String, Integer>();
    private final Map<String, FibonacciResult> resultInfos = new HashMap<String, FibonacciResult>();
    private final Map<String, Exception> failureInfos = new HashMap<String, Exception>();
    
    @BeforeScenario
    public void beforeEachScenario() {
        commanders.clear();
        requestInfos.clear();
        resultInfos.clear();
        failureInfos.clear();
    }
    
    @Given("a Commander named '$commanderName' with default properties file")
    public void createCommander(@Named("commanderName") final String commanderName) throws OpflowBootstrapException {
        commanders.put(commanderName, OpflowBuilder.createCommander("commander.properties"));
        if (LOG.isDebugEnabled()) LOG.debug("Commander[" + commanderName + "] has been created");
    }
    
    @Given("a Commander named '$commanderName' with properties file: '$propFile'")
    public void createCommander(@Named("commanderName") final String commanderName, 
            @Named("propFile") final String propFile) throws OpflowBootstrapException {
        commanders.put(commanderName, OpflowBuilder.createCommander(propFile));
        if (LOG.isDebugEnabled()) LOG.debug("Commander[" + commanderName + "] has been created");
    }
    
    @Given("a registered FibonacciCalculator interface in Commander named '$commanderName'")
    public void instantiateFibonacciCalculator(@Named("commanderName") final String commanderName)
            throws OpflowBootstrapException {
        commanders.get(commanderName).registerType(FibonacciCalculator.class);
    }
    
    @When("I send a request to Commander '$commanderName' to calculate fibonacci of '$number'")
    public void callCommanderCalcMethod(@Named("commanderName") final String commanderName,
            @Named("number") final int number) throws OpflowBootstrapException {
        FibonacciCalculator calculator = commanders.get(commanderName).registerType(FibonacciCalculator.class);
        FibonacciResult singleResult = calculator.calc(new FibonacciPacket(number));
        if (LOG.isDebugEnabled()) LOG.debug("Fibonacci(" + number + ") = " + singleResult.getValue());
        resultInfos.put(commanderName, singleResult);
    }
    
    @Then("the fibonacci value of '$number' by Commander '$commanderName' must be '$result'")
    public void verifyCommanderCalcMethod(@Named("commanderName") final String commanderName,
            @Named("number") final int number,
            @Named("result") final int result) throws OpflowBootstrapException {
        FibonacciResult singleResult = resultInfos.get(commanderName);
        assertThat(singleResult.getNumber(), equalTo(number));
        assertThat((int)singleResult.getValue(), equalTo(result));
    }
    
    @When("I make '$total' requests to Commander '$commanderName' to calculate fibonacci of random number from '$from' to '$to'")
    public void callBatchOfCommanderCalcMethods(@Named("commanderName") final String commanderName,
            @Named("total") final int total,
            @Named("from") final int minBound,
            @Named("to") final int maxBound) throws OpflowBootstrapException {
        FibonacciCalculator calculator = commanders.get(commanderName).registerType(FibonacciCalculator.class);
        for(int i=0; i<total; i++) {
            int number = FibonacciUtil.random(minBound, maxBound);
            String callId = commanderName + "-rand#" + i;
            requestInfos.put(callId, number);
            try {
                resultInfos.put(callId, calculator.calc(number));
            } catch (Exception exception) {
                failureInfos.put(callId, exception);
                if (LOG.isDebugEnabled()) LOG.debug("Exception: " + exception.getClass().getName());
            }
        }
        if (LOG.isDebugEnabled()) LOG.debug("batched calculation has been done");
    }
    
    @Then("the commander '$commanderName' will receive '$total' responses")
    public void verifyBatchOfCommanderCalcMethods(@Named("commanderName") final String commanderName,
            @Named("total") final int total) throws OpflowBootstrapException {
        assertThat(resultInfos.size() + failureInfos.size(), equalTo(total));
    }
    
    @When("I close Commander named '$commanderName'")
    public void closeCommander(@Named("commanderName") String commanderName) {
        commanders.get(commanderName).close();
    }
}
