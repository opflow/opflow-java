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
    Map<String, Object> activateAllPublishers(boolean state, Map<String, Object> opts);
    Map<String, Object> activatePublisher(String connectorName, boolean state, Map<String, Object> opts);
    Map<String, Object> activateAllNativeWorkers(boolean state, Map<String, Object> opts);
    Map<String, Object> activateNativeWorker(String connectorName, boolean state, Map<String, Object> opts);
    Map<String, Object> activateAllRemoteAMQPWorkers(boolean state, Map<String, Object> opts);
    Map<String, Object> activateRemoteAMQPWorker(String connectorName, boolean state, Map<String, Object> opts);
    Map<String, Object> activateAllRemoteHTTPWorkers(boolean state, Map<String, Object> opts);
    Map<String, Object> activateRemoteHTTPWorker(String connectorName, boolean state, Map<String, Object> opts);
    Map<String, Object> resetRpcInvocationCounter();
    Map<String, Object> resetDiscoveryClient();
}
