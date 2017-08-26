package com.devebot.opflow.tdd;

import com.devebot.opflow.OpflowEngine;
import com.devebot.opflow.OpflowExecutor;
import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public abstract class OpflowAbstractTest {
    public static final String[] TEST_EXCHANGE_NAMES = new String[] {
        "tdd-opflow-exchange", "tdd-opflow-publisher"
    };
    
    public static final String[] TEST_QUEUE_NAMES = new String[] {
        "tdd-opflow-queue", "tdd-opflow-operator", "tdd-opflow-response",
        "tdd-opflow-feedback", "tdd-opflow-subscriber", "tdd-opflow-recyclebin"
    };
    
    private static OpflowExecutor executor;
    
    protected static void clearTestExchanges(String uri) throws OpflowBootstrapException {
        for(String name: TEST_EXCHANGE_NAMES) {
            getExecutor(uri).deleteExchange(name);
        }
    }
    
    protected static void clearTestQueues(String uri) throws OpflowBootstrapException {
        for(String name: TEST_QUEUE_NAMES) {
            getExecutor(uri).deleteQueue(name);
        }
    }
    
    private static OpflowExecutor getExecutor(String uri) throws OpflowBootstrapException {
        if (executor == null) {
            Map<String, Object> params = new HashMap<String, Object>();
            params.put("uri", uri);
            OpflowEngine engine = new OpflowEngine(params);
            executor = new OpflowExecutor(engine);
        }
        return executor;
    }
}
