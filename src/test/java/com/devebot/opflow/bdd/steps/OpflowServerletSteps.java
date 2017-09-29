package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowBuilder;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.lab.FibonacciCalculatorImpl;
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
public class OpflowServerletSteps {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowServerletSteps.class);
    private final Map<String, OpflowServerlet> serverlets = new HashMap<String, OpflowServerlet>();
    
    @BeforeScenario
    public void beforeEachScenario() {
        serverlets.clear();
    }
    
    @Given("a Serverlet named '$serverletName' with default properties file")
    public void createServerlet(@Named("serverletName") final String serverletName) throws OpflowBootstrapException {
        serverlets.put(serverletName, OpflowBuilder.createServerlet());
        if (LOG.isDebugEnabled()) LOG.debug("Serverlet[" + serverletName + "] has been created");
    }
    
    @Given("a Serverlet named '$serverletName' with properties file: '$propFile'")
    public void createServerlet(@Named("serverletName") final String serverletName, 
            @Named("propFile") final String propFile) throws OpflowBootstrapException {
        serverlets.put(serverletName, OpflowBuilder.createServerlet(propFile));
        if (LOG.isDebugEnabled()) LOG.debug("Serverlet[" + serverletName + "] has been created");
    }
    
    @Given("an instantiated FibonacciCalculator class in Serverlet named '$serverletName'")
    public void instantiateFibonacciCalculator(@Named("serverletName") final String serverletName)
            throws OpflowBootstrapException {
        serverlets.get(serverletName).instantiateType(FibonacciCalculatorImpl.class);
    }
    
    @When("I close Serverlet named '$serverletName'")
    public void closeServerlet(@Named("serverletName") String serverletName) {
        serverlets.get(serverletName).close();
    }
}
