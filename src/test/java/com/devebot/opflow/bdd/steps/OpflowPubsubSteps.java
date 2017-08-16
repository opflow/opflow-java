package com.devebot.opflow.bdd.steps;

import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowPubsubListener;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowConstructorException;
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
    
    private final Map<String, OpflowPubsubHandler> pubsubs =  new HashMap<String, OpflowPubsubHandler>();
    private final Map<String, Integer> messageTotal =  new HashMap<String, Integer>();
    private final Map<String, Integer> counter =  new ConcurrentHashMap<String, Integer>();
    
    @Given("a PubsubHandler named '$pubsubName'")
    public void createPubsubHandler(@Named("pubsubName") final String pubsubName) throws OpflowConstructorException {
        pubsubs.put(pubsubName, OpflowHelper.createPubsubHandler());
        counter.put(pubsubName, 0);
    }
    
    @Given("a subscriber named '$names' in PubsubHandler named '$pubsubName'")
    public void consumeEchoJsonObject(@Named("names") final String names, @Named("pubsubName") final String pubsubName) {
        OpflowPubsubListener listener = null;
        
        if ("EchoJsonObject".equals(names)) {
            listener = new OpflowPubsubListener() {
                @Override
                public void processMessage(OpflowMessage message) throws IOException {
                    try {
                        String msg = message.getContentAsString();
                        if (LOG.isTraceEnabled()) LOG.trace("[+] EchoJsonObject received: '" + msg + "'");
                        counter.put(pubsubName, counter.get(pubsubName) + 1);
                        if (counter.get(pubsubName) >= messageTotal.get(pubsubName)) {
                            lock.lock();
                            try {
                                done.signal();
                            } finally {
                                lock.unlock();
                            }
                        }
                    } catch (final Exception ex) {
                        String errmsg = OpflowUtil.buildJson(new OpflowUtil.MapListener() {
                            @Override
                            public void transform(Map<String, Object> opts) {
                                opts.put("exceptionClass", ex.getClass().getName());
                                opts.put("exceptionMessage", ex.getMessage());
                            }
                        });
                        if (LOG.isErrorEnabled()) LOG.error("[-] EchoJsonObject error message: " + errmsg);
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
    
    @Then("PubsubHandler named '$pubsubName' receives '$total' messages")
    public void totalReceivedMessages(@Named("pubsubName") final String pubsubName, 
            @Named("total") final int total) {
        lock.lock();
        try {
            done.await();
            assertThat(counter.get(pubsubName), equalTo(total));
        } catch(InterruptedException ie) {
        } finally {
            lock.unlock();
        }
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
