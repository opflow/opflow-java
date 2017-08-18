package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowPubsubListener;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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
    
    private final Lock lock = new ReentrantLock();
    private final Condition done = lock.newCondition();
    private boolean running = true;
    
    private final Map<String, OpflowPubsubHandler> pubsubs =  new HashMap<String, OpflowPubsubHandler>();
    private final Map<String, Integer> messageTotal = new HashMap<String, Integer>();
    private final Map<String, Integer> counter =  new ConcurrentHashMap<String, Integer>();
    private final Integer[] rejected = new Integer[] { 15, 25, 35, 55, 95 };
    
    @Given("a PubsubHandler named '$pubsubName'")
    public void createPubsubHandler(@Named("pubsubName") final String pubsubName) throws OpflowConstructorException {
        pubsubs.put(pubsubName, OpflowHelper.createPubsubHandler());
        counter.put(pubsubName, 0);
    }
    
    @Given("a PubsubHandler named '$pubsubName' with properties file: '$propFile'")
    public void createPubsubHandler(@Named("pubsubName") final String pubsubName, 
            @Named("propFile") final String propFile) throws OpflowConstructorException {
        pubsubs.put(pubsubName, OpflowHelper.createPubsubHandler(propFile));
        counter.put(pubsubName, 0);
    }
    
    @Given("a subscriber named '$names' in PubsubHandler named '$pubsubName'")
    public void consumeEchoJsonObject(@Named("names") final String names, @Named("pubsubName") final String pubsubName) {
        OpflowPubsubListener listener = null;
        
        if ("EchoJsonObject".equals(names)) {
            listener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    String msg = message.getContentAsString();
                    if (LOG.isTraceEnabled()) LOG.trace("[+] EchoJsonObject received: '" + msg + "'");
                    counter.put(pubsubName, counter.get(pubsubName) + 1);
                    if (counter.get(pubsubName) >= messageTotal.get(pubsubName)) {
                        lock.lock();
                        try {
                            running = false;
                            done.signal();
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            };
        }
        
        if ("EchoRandomError".equals(names)) {
            listener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    counter.put(pubsubName, counter.get(pubsubName) + 1);
                    String msg = message.getContentAsString();
                    int current = counter.get(pubsubName);
                    int total = messageTotal.get(pubsubName);
                    if (LOG.isTraceEnabled()) LOG.trace("[+] EchoRandomError received: '" + msg + "'/" + current);
                    Map<String, Object> msgObj = OpflowUtil.jsonStringToMap(msg);
                    Integer number = ((Double) msgObj.get("number")).intValue();
                    try {
                        if (OpflowUtil.arrayContains(rejected, number)) {
                            throw new OpflowOperationException("Invalid number: " + number);
                        }
                    } finally {
                        if (current >= (total + 3 * rejected.length)) {
                            lock.lock();
                            try {
                                running = false;
                                done.signal();
                            } finally {
                                lock.unlock();
                            }
                        }
                    }
                }
            };
        }
        
        if (listener != null) pubsubs.get(pubsubName).subscribe(listener);
    }
    
    @When("I publish '$total' random messages to PubsubHandler named '$pubsubName'")
    public void totalReceivedMessages(@Named("total") final int total, 
            @Named("pubsubName") final String pubsubName) {
        messageTotal.put(pubsubName, total);
        running = true;
        for(int i = 0; i < total; i++) {
            final int number = i;
            pubsubs.get(pubsubName).publish(OpflowUtil.buildJson(new OpflowUtil.MapListener() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put("number", number);
                }
            }));
        }
    }
    
    @When("waiting for subscriber of PubsubHandler($pubsubName) finish")
    public void waitSubscriberFinish(@Named("pubsubName") final String pubsubName) {
        lock.lock();
        try {
            while (running) done.await();
        } catch(InterruptedException ie) {
        } finally {
            lock.unlock();
        }
    }
    
    @Then("PubsubHandler named '$pubsubName' receives '$total' messages")
    public void totalReceivedMessages(@Named("pubsubName") final String pubsubName, 
            @Named("total") final int total) {
        assertThat(counter.get(pubsubName), equalTo(total));
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
        assertThat(collection, hasItem(status));
        if ("opened".equals(status)) {
            assertThat(OpflowPubsubHandler.State.CONNECTION_OPENED, equalTo(state.getConnectionState()));
        } else if ("closed".equals(status)) {
            assertThat(OpflowPubsubHandler.State.CONNECTION_CLOSED, equalTo(state.getConnectionState()));
        }
    }
}
