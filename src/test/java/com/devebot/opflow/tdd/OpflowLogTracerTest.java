package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowLogTracer;
import com.google.gson.Gson;
import java.util.HashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowLogTracerTest {
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowLogTracerTest.class);
    
    @Test
    public void testCopyMethod() {
        System.setProperty("OPFLOW_INSTANCE_ID", "demo");
        
        OpflowLogTracer logTracer = new OpflowLogTracer().branch("rootId", "1234567890");
        
        String empty = logTracer.toString();
        
        assertThat(empty, equalTo("{\"instanceId\":\"demo\",\"rootId\":\"1234567890\"}"));
        
        String logmsg = logTracer
                .put("message", "Hello Opflow")
                .toString();
        
        assertThat(logmsg, equalTo("{\"message\":\"Hello Opflow\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\"}"));
        
        OpflowLogTracer subTracer1 = logTracer.reset()
                .put("appId", "app1");
        
        String logmsg1 = subTracer1
                .put("message", "Hello App1")
                .toString();
        
        OpflowLogTracer subTracer2 = logTracer.reset()
                .put("appId", "app2");
        
        String logmsg2 = subTracer2
                .put("message", "Hello App2")
                .toString();
        
        assertThat(logmsg1, equalTo("{\"message\":\"Hello App1\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\",\"appId\":\"app1\"}"));
        assertThat(logmsg2, equalTo("{\"message\":\"Hello App2\",\"instanceId\":\"demo\",\"rootId\":\"1234567890\",\"appId\":\"app2\"}"));
        
        LOG.debug("logmsg1: " + logmsg1);
        LOG.debug("logmsg2: " + logmsg2);
    }

    @Test
    public void testBranchAndReset1Method() {
        System.setProperty("OPFLOW_INSTANCE_ID", "demo");
        
        OpflowLogTracer logTracer = new OpflowLogTracer();
        
        OpflowLogTracer logEngine = logTracer.branch("engineId", "engine-demo");
        String msgEngine = logEngine.toString();
        assertThat(msgEngine, equalTo("{\"instanceId\":\"demo\",\"engineId\":\"engine-demo\"}"));
        LOG.debug("msgEngine: " + msgEngine);
        
        OpflowLogTracer logApp1 = logEngine
                .put("appId", "app1")
                .put("message", "Hello App1");
        String msgApp1 = logApp1.toString();
        assertThat(msgApp1, equalTo("{\"message\":\"Hello App1\",\"instanceId\":\"demo\",\"engineId\":\"engine-demo\",\"appId\":\"app1\"}"));
        
        OpflowLogTracer logApp2 = logEngine.reset()
                .put("appID", "app2")
                .put("content", "Hello App2");
        String msgApp2 = logApp2.toString();
        assertThat(msgApp2, equalTo("{\"instanceId\":\"demo\",\"engineId\":\"engine-demo\",\"appID\":\"app2\",\"content\":\"Hello App2\"}"));
    }
    
    @Test
    public void testGsonToJsonMethod() {
        Gson gson = new Gson();
        OpflowLogTracer logObject = new OpflowLogTracer();
        
        logObject.put("engineId", "engine1").put("consumerId", "consumer1");
        
        HashMap<String, Object> A = new HashMap<String, Object>();
        
        A.put("A1", "Value 1");
        A.put("A2", "Value 2");
        
        HashMap<String, Object> A2 = new HashMap<String, Object>();
        
        A2.put("A2_1", "Sub 1");
        A2.put("A2_2", "Sub 2");
        
        A.put("A2", A2);
        
        HashMap<String, String> B = (HashMap<String, String>) A.clone();
        
        A2.put("A2_2", "Sub 9");
        
        LOG.debug("Json string: " + gson.toJson(B));
    }
}
