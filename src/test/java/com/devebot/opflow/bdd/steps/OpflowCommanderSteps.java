package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowCommander;
import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.lab.FibonacciCalculator;
import com.devebot.opflow.lab.FibonacciPacket;
import com.devebot.opflow.lab.FibonacciResult;
import java.util.HashMap;
import java.util.Map;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.When;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowCommanderSteps {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommanderSteps.class);
    private final Map<String, OpflowCommander> commanders = new HashMap<String, OpflowCommander>();
    
    @BeforeScenario
    public void beforeEachScenario() {
        commanders.clear();
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
    public void callCommanderCalc(@Named("commanderName") final String commanderName,
            @Named("number") final int number) throws OpflowBootstrapException {
        FibonacciCalculator calculator = commanders.get(commanderName).registerType(FibonacciCalculator.class);
        FibonacciResult result = calculator.calc(new FibonacciPacket(number));
        System.out.println("XXXXXXXXXXXXXXXXXX: " + result.getValue());
    }
    
    @When("I close Commander named '$commanderName'")
    public void closeCommander(@Named("commanderName") String commanderName) {
        commanders.get(commanderName).close();
    }
}
