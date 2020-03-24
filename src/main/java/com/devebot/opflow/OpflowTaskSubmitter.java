package com.devebot.opflow;

import java.util.Map;

/**
 *
 * @author drupalex
 */
public interface OpflowTaskSubmitter {
    Map<String, Object> pause(long duration);
    Map<String, Object> unpause();
    Map<String, Object> reset();
    Map<String, Object> activatePublisher(boolean state, Map<String, Object> opts);
    Map<String, Object> activateNativeWorker(boolean state, Map<String, Object> opts);
    Map<String, Object> activateRemoteAMQPWorker(boolean state, Map<String, Object> opts);
    Map<String, Object> activateRemoteHTTPWorker(boolean state, Map<String, Object> opts);
    Map<String, Object> resetRpcInvocationCounter();
}
