package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowEngine;
import com.devebot.opflow.OpflowExecutor;
import com.devebot.opflow.OpflowHelper;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowPubsubListener;
import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.exception.OpflowOperationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 *
 * @author drupalex
 */
public class OpflowPubsubHandlerTest {
    Properties props;
    OpflowPubsubHandler pubsub;
    
    @Before
    public void beforeEach() throws OpflowConstructorException {
        props = OpflowHelper.loadProperties();
    }
    
    @After
    public void afterEach() {
        if (pubsub != null) pubsub.close();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testConstructorWithNullOperatorName() throws OpflowConstructorException {
        thrown.expect(OpflowConstructorException.class);
        thrown.expectMessage(CoreMatchers.is("subscriberName must not be null"));
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        pubsub = new OpflowPubsubHandler(pars);
    }
    
    @Test
    public void testConstructorAutoCreateQueues() throws OpflowConstructorException {
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        OpflowEngine engine = new OpflowEngine(pars);
        OpflowExecutor executor = new OpflowExecutor(engine);
        executor.deleteQueue("tdd-opflow-subscriber");
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        pars.put("subscriberName", "tdd-opflow-subscriber");
        pubsub = new OpflowPubsubHandler(pars);
    }
    
    @Test
    public void testSubscribeWithNullListener() throws OpflowConstructorException {
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        pars.put("subscriberName", "tdd-opflow-subscriber");
        pubsub = new OpflowPubsubHandler(pars);
        
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(CoreMatchers.is("PubsubListener should not be null"));
        pubsub.subscribe(null);
    }
    
    @Test
    public void testSubscribeWithDifferentListeners() throws OpflowConstructorException {
        Map<String, Object> pars = new HashMap<String, Object>();
        pars.put("uri", props.get("opflow.uri"));
        pars.put("exchangeName", "tdd-opflow-exchange");
        pars.put("routingKey", "tdd-opflow-rpc");
        pars.put("subscriberName", "tdd-opflow-subscriber");
        pubsub = new OpflowPubsubHandler(pars);
        
        thrown.expect(OpflowOperationException.class);
        thrown.expectMessage(CoreMatchers.is("PubsubHandler only supports single PubsubListener"));
        pubsub.subscribe(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
            }
        });
        pubsub.subscribe(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
            }
        });
    }
}
