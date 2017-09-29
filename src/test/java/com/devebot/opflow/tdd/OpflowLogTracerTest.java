package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowJsontool;
import com.devebot.opflow.OpflowLogTracer;
import com.google.gson.Gson;
import java.util.HashMap;
import java.util.Map;
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
    public void testResetMethod() {
        System.setProperty("OPFLOW_INSTANCE_ID", "demo");
        
        OpflowLogTracer logTracer = new OpflowLogTracer().branch("rootId", "1234567890");
        
        String emptyMsg = logTracer.toString();
        Map<String, Object> emptyMap = OpflowJsontool.toObjectMap(emptyMsg);
        
        assertThat(emptyMap.get("instanceId").toString(), equalTo("demo"));
        assertThat(emptyMap.get("rootId").toString(), equalTo("1234567890"));
        
        String logMsg0 = logTracer
                .put("message", "Hello Opflow")
                .toString();
        Map<String, Object> logMap0 = OpflowJsontool.toObjectMap(logMsg0);
        
        assertThat(logMap0.get("instanceId").toString(), equalTo("demo"));
        assertThat(logMap0.get("rootId").toString(), equalTo("1234567890"));
        assertThat(logMap0.get("message").toString(), equalTo("Hello Opflow"));
        
        OpflowLogTracer subTracer1 = logTracer.reset()
                .put("appId", "app1");
        
        String logMsg1 = subTracer1
                .put("message", "Hello App1")
                .toString();
        
        Map<String, Object> logMap1 = OpflowJsontool.toObjectMap(logMsg1);
        
        assertThat(logMap1.get("instanceId").toString(), equalTo("demo"));
        assertThat(logMap1.get("rootId").toString(), equalTo("1234567890"));
        assertThat(logMap1.get("message").toString(), equalTo("Hello App1"));
        assertThat(logMap1.get("appId").toString(), equalTo("app1"));
        
        OpflowLogTracer subTracer2 = logTracer.reset()
                .put("appId", "app2");
        
        String logMsg2 = subTracer2
                .put("message", "Hello App2")
                .toString();
        
        Map<String, Object> logMap2 = OpflowJsontool.toObjectMap(logMsg2);
        
        assertThat(logMap2.get("instanceId").toString(), equalTo("demo"));
        assertThat(logMap2.get("rootId").toString(), equalTo("1234567890"));
        assertThat(logMap2.get("message").toString(), equalTo("Hello App2"));
        assertThat(logMap2.get("appId").toString(), equalTo("app2"));
        
        LOG.debug("logmsg1: " + logMsg1);
        LOG.debug("logmsg2: " + logMsg2);
    }

    @Test
    public void testBranchAndReset1Method() {
        System.setProperty("OPFLOW_INSTANCE_ID", "demo");
        
        OpflowLogTracer logTracer = new OpflowLogTracer();
        
        OpflowLogTracer logEngine = logTracer.branch("engineId", "engine-demo");
        String msgEngine = logEngine.toString();
        Map<String, Object> mapEngine = OpflowJsontool.toObjectMap(msgEngine);
        assertThat(mapEngine.get("instanceId").toString(), equalTo("demo"));
        assertThat(mapEngine.get("engineId").toString(), equalTo("engine-demo"));
        LOG.debug("msgEngine: " + msgEngine);
        
        OpflowLogTracer logApp1 = logEngine
                .put("appId", "app1")
                .put("message", "Hello App1");
        String msgApp1 = logApp1.toString();
        Map<String, Object> mapApp1 = OpflowJsontool.toObjectMap(msgApp1);
        assertThat(mapApp1.get("instanceId").toString(), equalTo("demo"));
        assertThat(mapApp1.get("engineId").toString(), equalTo("engine-demo"));
        assertThat(mapApp1.get("message").toString(), equalTo("Hello App1"));
        assertThat(mapApp1.get("appId").toString(), equalTo("app1"));
        
        OpflowLogTracer logApp2 = logEngine.reset()
                .put("appID", "app2")
                .put("content", "Hello App2");
        String msgApp2 = logApp2.toString();
        Map<String, Object> mapApp2 = OpflowJsontool.toObjectMap(msgApp2);
        assertThat(mapApp2.get("instanceId").toString(), equalTo("demo"));
        assertThat(mapApp2.get("engineId").toString(), equalTo("engine-demo"));
        assertThat(mapApp2.get("content").toString(), equalTo("Hello App2"));
        assertThat(mapApp2.get("appID").toString(), equalTo("app2"));
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
