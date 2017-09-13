package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLoader;
import com.devebot.opflow.OpflowMessage;
import com.devebot.opflow.OpflowPubsubListener;
import com.devebot.opflow.OpflowRpcListener;
import com.devebot.opflow.OpflowRpcResponse;
import com.devebot.opflow.OpflowServerlet;
import com.devebot.opflow.exception.OpflowBootstrapException;
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
public class OpflowServerletTest {
    Properties props;
    OpflowServerlet serverlet;
    
    @Before
    public void beforeEach() throws OpflowBootstrapException {
        props = OpflowLoader.loadProperties();
    }
    
    @After
    public void afterEach() {
        if (serverlet != null) serverlet.stop();
    }
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    
    @Test
    public void testDuplicated_Subscriber_ExchangeName_RoutingKey() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> subscriberCfg = new HashMap<String, Object>();
        subscriberCfg.put("uri", props.getProperty("opflow.uri"));
        subscriberCfg.put("exchangeName", "tdd-opflow-publisher");
        subscriberCfg.put("routingKey", "tdd-opflow-configurer");
        subscriberCfg.put("subscriberName", "tdd-opflow-subscriber");
        subscriberCfg.put("recyclebinName", "tdd-opflow-recyclebin");
        params.put("subscriber", subscriberCfg);
        
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Duplicated Subscriber connection parameters"));
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }), params);
    }
    
    @Test
    public void testDuplicated_Subscriber_RecyclebinName1() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> subscriberCfg = new HashMap<String, Object>();
        subscriberCfg.put("uri", props.getProperty("opflow.uri"));
        subscriberCfg.put("exchangeName", "tdd-opflow-publisher");
        subscriberCfg.put("routingKey", "tdd-opflow-pubsub-public");
        subscriberCfg.put("subscriberName", "tdd-opflow-subscriber");
        subscriberCfg.put("recyclebinName", "tdd-opflow-subscriber");
        params.put("subscriber", subscriberCfg);
        
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Invalid recyclebinName (duplicated with some queueNames)"));
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }), params);
    }
    
    @Test
    public void testDuplicated_Subscriber_RecyclebinName2() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> rpcWorkerCfg = new HashMap<String, Object>();
        rpcWorkerCfg.put("uri", props.getProperty("opflow.uri"));
        rpcWorkerCfg.put("exchangeName", "tdd-opflow-exchange");
        rpcWorkerCfg.put("routingKey", "tdd-opflow-rpc");
        rpcWorkerCfg.put("operatorName", "tdd-opflow-operator");
        rpcWorkerCfg.put("responseName", "tdd-opflow-response");
        params.put("rpcWorker", rpcWorkerCfg);
        
        Map<String, Object> subscriberCfg = new HashMap<String, Object>();
        subscriberCfg.put("uri", props.getProperty("opflow.uri"));
        subscriberCfg.put("exchangeName", "tdd-opflow-publisher");
        subscriberCfg.put("routingKey", "tdd-opflow-pubsub-public");
        subscriberCfg.put("subscriberName", "tdd-opflow-subscriber");
        subscriberCfg.put("recyclebinName", "tdd-opflow-response");
        params.put("subscriber", subscriberCfg);
        
        thrown.expect(OpflowBootstrapException.class);
        thrown.expectMessage(CoreMatchers.startsWith("Invalid recyclebinName (duplicated with some queueNames)"));
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowServerlet.RpcWorkerEntry[] {
            new OpflowServerlet.RpcWorkerEntry("fibonacci", new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            })
        }, new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }), params);
    }
    
    @Test
    public void testConstructor1() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> subscriberCfg = new HashMap<String, Object>();
        subscriberCfg.put("uri", props.getProperty("opflow.uri"));
        subscriberCfg.put("exchangeName", "tdd-opflow-publisher");
        subscriberCfg.put("routingKey", "tdd-opflow-pubsub-public");
        subscriberCfg.put("subscriberName", "tdd-opflow-subscriber");
        subscriberCfg.put("recyclebinName", "tdd-opflow-recyclebin");
        params.put("subscriber", subscriberCfg);
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }), params);
    }
    
    @Test
    public void testConstructor2() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> rpcWorkerCfg = new HashMap<String, Object>();
        rpcWorkerCfg.put("uri", props.getProperty("opflow.uri"));
        rpcWorkerCfg.put("exchangeName", "tdd-opflow-exchange");
        rpcWorkerCfg.put("routingKey", "tdd-opflow-rpc");
        rpcWorkerCfg.put("operatorName", "tdd-opflow-operator");
        rpcWorkerCfg.put("responseName", "tdd-opflow-response");
        params.put("rpcWorker", rpcWorkerCfg);
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowServerlet.RpcWorkerEntry[] {
            new OpflowServerlet.RpcWorkerEntry("fibonacci", new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            })
        }), params);
    }
    
    @Test
    public void testConstructor3() throws OpflowBootstrapException {
        Map<String, Object> params = new HashMap<String, Object>();
        
        Map<String, Object> configurerCfg = new HashMap<String, Object>();
        configurerCfg.put("uri", props.getProperty("opflow.uri"));
        configurerCfg.put("exchangeName", "tdd-opflow-publisher");
        configurerCfg.put("routingKey", "tdd-opflow-configurer");
        params.put("configurer", configurerCfg);
        
        Map<String, Object> rpcWorkerCfg = new HashMap<String, Object>();
        rpcWorkerCfg.put("uri", props.getProperty("opflow.uri"));
        rpcWorkerCfg.put("exchangeName", "tdd-opflow-exchange");
        rpcWorkerCfg.put("routingKey", "tdd-opflow-rpc");
        rpcWorkerCfg.put("operatorName", "tdd-opflow-operator");
        rpcWorkerCfg.put("responseName", "tdd-opflow-response");
        params.put("rpcWorker", rpcWorkerCfg);
        
        Map<String, Object> subscriberCfg = new HashMap<String, Object>();
        subscriberCfg.put("uri", props.getProperty("opflow.uri"));
        subscriberCfg.put("exchangeName", "tdd-opflow-publisher");
        subscriberCfg.put("routingKey", "tdd-opflow-pubsub-public");
        subscriberCfg.put("subscriberName", "tdd-opflow-subscriber");
        subscriberCfg.put("recyclebinName", "tdd-opflow-recyclebin");
        params.put("subscriber", subscriberCfg);
        
        serverlet = new OpflowServerlet(new OpflowServerlet.ListenerMap(new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }, new OpflowServerlet.RpcWorkerEntry[] {
            new OpflowServerlet.RpcWorkerEntry("fibonacci", new OpflowRpcListener() {
                @Override
                public Boolean processMessage(OpflowMessage message, OpflowRpcResponse response) throws IOException {
                    throw new UnsupportedOperationException("Not supported yet.");
                }
            })
        }, new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowMessage message) throws IOException {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        }), params);
    }
}
