package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowPubsubListener;
import com.devebot.opflow.OpflowTask;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import org.jbehave.core.annotations.BeforeScenario;
import org.jbehave.core.annotations.Given;
import org.jbehave.core.annotations.Named;
import org.jbehave.core.annotations.Then;
import org.jbehave.core.annotations.When;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubSteps {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPubsubSteps.class);
    
    private final Map<String, OpflowPubsubHandler> pubsubs =  new HashMap<String, OpflowPubsubHandler>();
    private final Map<String, OpflowTask.Countdown> countdowns =  new HashMap<String, OpflowTask.Countdown>();
    private final Integer[] rejected = new Integer[] { 15, 25, 35, 55, 95 };
    
    @BeforeScenario
    public void beforeEachScenario() {
        pubsubs.clear();
        countdowns.clear();
    }
    
    @Given("a PubsubHandler named '$pubsubName' with default config")
    public void createPubsubHandler(@Named("pubsubName") final String pubsubName) throws OpflowBootstrapException {
        pubsubs.put(pubsubName, OpflowLoader.createPubsubHandler());
        countdowns.put(pubsubName, new OpflowTask.Countdown(0, 1000));
        if (LOG.isDebugEnabled()) LOG.debug("PubsubHandler[" + pubsubName + "] has been created");
    }
    
    @Given("a PubsubHandler named '$pubsubName' with properties file: '$propFile'")
    public void createPubsubHandler(@Named("pubsubName") final String pubsubName, 
            @Named("propFile") final String propFile) throws OpflowBootstrapException {
        pubsubs.put(pubsubName, OpflowLoader.createPubsubHandler(propFile));
        countdowns.put(pubsubName, new OpflowTask.Countdown(0, 1000));
        if (LOG.isDebugEnabled()) LOG.debug("PubsubHandler[" + pubsubName + "] has been created");
    }
    
    @Given("a subscriber named '$names' in PubsubHandler named '$pubsubName'")
    public void consumeEchoJsonObject(@Named("names") final String names, @Named("pubsubName") final String pubsubName) {
        OpflowPubsubListener listener = null;
        
        if ("EchoJsonObject".equals(names)) {
            listener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    String msg = message.getBodyAsString();
                    if (LOG.isTraceEnabled()) LOG.trace("[+] EchoJsonObject received: '" + msg + "'");
                    countdowns.get(pubsubName).check();
                }
            };
        }
        
        if ("EchoRandomError".equals(names)) {
            listener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    String msg = message.getBodyAsString();
                    if (LOG.isTraceEnabled()) LOG.trace("[+] EchoRandomError received: '" + msg + "'" +
                            " #" + countdowns.get(pubsubName).getCount());
                    Map<String, Object> msgObj = OpflowUtil.jsonStringToMap(msg);
                    Integer number = ((Double) msgObj.get("number")).intValue();
                    try {
                        if (OpflowUtil.arrayContains(rejected, number)) {
                            throw new OpflowOperationException("Invalid number: " + number);
                        }
                    } finally {
                        countdowns.get(pubsubName).check();
                    }
                }
            };
        }
        
        if (listener != null) pubsubs.get(pubsubName).subscribe(listener);
    }
    
    @When("I publish '$total' random messages to subscriber '$names' on PubsubHandler named '$pubsubName'")
    public void totalReceivedMessages(@Named("names") final String names, @Named("total") final int total, 
            @Named("pubsubName") final String pubsubName) {
        if ("EchoJsonObject".equals(names)) {
            countdowns.get(pubsubName).reset(total);
        }
        if ("EchoRandomError".equals(names)) {
            countdowns.get(pubsubName).reset(total + 3 * rejected.length);
        }
        for(int i = 0; i < total; i++) {
            final int number = i;
            pubsubs.get(pubsubName).publish(OpflowUtil.buildMap().put("number", number).toString());
        }
    }
    
    @When("waiting for subscriber of PubsubHandler($pubsubName) finish")
    public void waitSubscriberFinish(@Named("pubsubName") final String pubsubName) {
        countdowns.get(pubsubName).bingo();
    }
    
    @Then("PubsubHandler named '$pubsubName' receives '$total' messages")
    public void totalReceivedMessages(@Named("pubsubName") final String pubsubName, 
            @Named("total") final int total) {
        assertThat(countdowns.get(pubsubName).getCount(), equalTo(total));
    }
    
    @When("I purge subscriber in PubsubHandler named '$pubsubName'")
    public void purgeSubscriber(@Named("pubsubName") final String pubsubName) {
        OpflowPubsubHandler pubsub = pubsubs.get(pubsubName);
        pubsub.getExecutor().purgeQueue(pubsub.getSubscriberName());
    }
    
    @Then("subscriber in PubsubHandler named '$pubsubName' has '$total' messages")
    public void countSubscriber(@Named("pubsubName") final String pubsubName, 
            @Named("total") final int total) {
        OpflowPubsubHandler pubsub = pubsubs.get(pubsubName);
        assertThat(pubsub.getExecutor().countQueue(pubsub.getSubscriberName()), equalTo(total));
    }
    
    @When("I purge recyclebin in PubsubHandler named '$pubsubName'")
    public void purgeRecyclebin(@Named("pubsubName") final String pubsubName) {
        OpflowPubsubHandler pubsub = pubsubs.get(pubsubName);
        pubsub.getExecutor().purgeQueue(pubsub.getRecyclebinName());
    }
    
    @Then("recyclebin in PubsubHandler named '$pubsubName' has '$total' messages")
    public void countRecyclebin(@Named("pubsubName") final String pubsubName, 
            @Named("total") final int total) {
        OpflowPubsubHandler pubsub = pubsubs.get(pubsubName);
        assertThat(pubsub.getExecutor().countQueue(pubsub.getRecyclebinName()), equalTo(total));
    }
    
    @When("I close PubsubHandler named '$pubsubName'")
    public void closePubsubHandler(@Named("pubsubName") String pubsubName) {
        pubsubs.get(pubsubName).close();
    }
    
    @Then("the PubsubHandler named '$pubsubName' connection is '$status'")
    public void checkPubsubHandler(@Named("pubsubName") String pubsubName, @Named("status") String status) {
        OpflowPubsubHandler.State state = pubsubs.get(pubsubName).check();
        List<String> collection = Lists.newArrayList("opened", "closed");
        if (LOG.isDebugEnabled()) LOG.debug("PubsubHandler[" + pubsubName + "] should have status: " + status);
        assertThat(collection, hasItem(status));
        if (LOG.isDebugEnabled()) LOG.debug("PubsubHandler[" + pubsubName + "] current state: " + state.getConnectionState());
        if ("opened".equals(status)) {
            assertThat(OpflowPubsubHandler.State.CONNECTION_OPENED, equalTo(state.getConnectionState()));
        } else if ("closed".equals(status)) {
            assertThat(OpflowPubsubHandler.State.CONNECTION_CLOSED, equalTo(state.getConnectionState()));
        }
    }
}
