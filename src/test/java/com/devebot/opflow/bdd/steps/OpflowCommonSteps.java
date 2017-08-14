package com.devebot.opflow.bdd.steps;

import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;

/**
 *
 * @author drupalex
 */
public class OpflowCommonSteps {
    @Given("a waiting time in $number seconds")
    public void doSomethingIn(@Named("number") final int number) {
        try {
            Thread.sleep(1000 * number);
        } catch (InterruptedException ie) {}
    }
}
