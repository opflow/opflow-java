package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowLogTracer;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.model.ExamplesTable;

/**
 *
 * @author drupalex
 */
public class OpflowCommonSteps {
    private static final Map<String, Integer> COUNTER = new HashMap<String, Integer>();
    
    @BeforeScenario
    public void beforeEachScenario() {
        COUNTER.clear();
    }
    
    @Given("a waiting time in $number seconds")
    public void doSomethingIn(@Named("number") final int number) {
        try {
            Thread.sleep(1000 * number);
        } catch (InterruptedException ie) {}
    }
    
    @Given("a stringify interceptor")
    public void givenStringifyInterceptor() {
        OpflowLogTracer.addStringifyInterceptor(new OpflowLogTracer.StringifyInterceptor() {
            @Override
            public void intercept(Map<String, Object> logdata) {
                if ("shared producingConnection is created".equals(logdata.get("message"))) {
                    COUNTER.put("sharedProducingConnectionCreated", COUNTER.getOrDefault("sharedProducingConnectionCreated", 0) + 1);
                }
                if ("shared producingConnection is closing".equals(logdata.get("message"))) {
                    COUNTER.put("sharedProducingConnectionClosed", COUNTER.getOrDefault("sharedProducingConnectionClosed", 0) + 1);
                }
                if ("shared producingChannel is created".equals(logdata.get("message"))) {
                    COUNTER.put("sharedProducingChannelCreated", COUNTER.getOrDefault("sharedProducingChannelCreated", 0) + 1);
                }
                if ("shared producingChannel is closing".equals(logdata.get("message"))) {
                    COUNTER.put("sharedProducingChannelClosed", COUNTER.getOrDefault("sharedProducingChannelClosed", 0) + 1);
                }
                
                if ("shared consumingConnection is created".equals(logdata.get("message"))) {
                    COUNTER.put("sharedConsumingConnectionCreated", COUNTER.getOrDefault("sharedConsumingConnectionCreated", 0) + 1);
                }
                if ("shared consumingConnection is closing".equals(logdata.get("message"))) {
                    COUNTER.put("sharedConsumingConnectionClosed", COUNTER.getOrDefault("sharedConsumingConnectionClosed", 0) + 1);
                }
                if ("shared consumingChannel is created".equals(logdata.get("message"))) {
                    COUNTER.put("sharedConsumingChannelCreated", COUNTER.getOrDefault("sharedConsumingChannelCreated", 0) + 1);
                }
                if ("shared consumingChannel is closing".equals(logdata.get("message"))) {
                    COUNTER.put("sharedConsumingChannelClosed", COUNTER.getOrDefault("sharedConsumingChannelClosed", 0) + 1);
                }
                
                if ("private consumingConnection is created".equals(logdata.get("message"))) {
                    COUNTER.put("privateConsumingConnectionCreated", COUNTER.getOrDefault("privateConsumingConnectionCreated", 0) + 1);
                }
                if ("private consumingConnection is closing".equals(logdata.get("message"))) {
                    COUNTER.put("privateConsumingConnectionClosed", COUNTER.getOrDefault("privateConsumingConnectionClosed", 0) + 1);
                }
                if ("private consumingChannel is created".equals(logdata.get("message"))) {
                    COUNTER.put("privateConsumingChannelCreated", COUNTER.getOrDefault("privateConsumingChannelCreated", 0) + 1);
                }
                if ("private consumingChannel is closing".equals(logdata.get("message"))) {
                    COUNTER.put("privateConsumingChannelClosed", COUNTER.getOrDefault("privateConsumingChannelClosed", 0) + 1);
                }
            }
        });
    }
    
    @Then("the COUNTER value of '$fieldName' must be '$value'")
    public void counterFieldNameEqualsValue(@Named("fieldName") final String fieldName,
            @Named("value") final Integer value) {
        assertThat(COUNTER.get(fieldName), equalTo(value));
    }
    
    @Then("the COUNTER value of '$fieldName1' must equal to value of '$fieldName2'")
    public void counterField1EqualsField2(@Named("fieldName1") final String fieldName1,
            @Named("fieldName2") final String fieldName2) {
        System.out.println("COUNTER: " + OpflowJsontool.toString(COUNTER));
        assertThat(COUNTER.get(fieldName1), equalTo(COUNTER.get(fieldName2)));
    }
    
    @Then("the COUNTER value of left field must equal to value of right field in table: $comparison")
    public void counterField1EqualsField2(@Named("comparison") final ExamplesTable comparison) {
        System.out.println("COUNTER: " + OpflowJsontool.toString(COUNTER));
        for (Map<String,String> row : comparison.getRows()) {
            String opened = row.get("opened");
            String closed = row.get("closed");
            assertThat(COUNTER.get(opened), equalTo(COUNTER.get(closed)));
        }
    }
}
