package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowBuilder {
    
    public static OpflowCommander createBroker() throws OpflowBootstrapException {
        return new OpflowCommander(OpflowObjectTree.buildMap().toMap());
    }
    
    public static OpflowCommander createCommander() throws OpflowBootstrapException {
        return createCommander(null, null, true);
    }
    
    public static OpflowCommander createCommander(String propFile) throws OpflowBootstrapException {
        return createCommander(null, propFile, true);
    }
    
    public static OpflowCommander createCommander(Map<String, Object> config) throws OpflowBootstrapException {
        return createCommander(config, null, false);
    }
    
    public static OpflowCommander createCommander(Map<String, Object> config,
            String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowCommander(new OpflowConfig.LoaderImplCommander(config, configFile, useDefaultFile));
    }
    
    public static OpflowServerlet createServerlet()
            throws OpflowBootstrapException {
        return createServerlet(OpflowServerlet.ListenerDescriptor.EMPTY, null, null, true);
    }
    
    public static OpflowServerlet createServerlet(String propFile)
            throws OpflowBootstrapException {
        return createServerlet(OpflowServerlet.ListenerDescriptor.EMPTY, null, propFile, true);
    }
    
    public static OpflowServerlet createServerlet(Map<String, Object> config)
            throws OpflowBootstrapException {
        return createServerlet(OpflowServerlet.ListenerDescriptor.EMPTY, config, null, false);
    }
    
    public static OpflowServerlet createServerlet(Map<String, Object> config, String configFile, boolean useDefaultFile)
            throws OpflowBootstrapException {
        return createServerlet(OpflowServerlet.ListenerDescriptor.EMPTY, config, configFile, useDefaultFile);
    }
    
    public static OpflowServerlet createServerlet(OpflowServerlet.ListenerDescriptor listeners)
            throws OpflowBootstrapException {
        return createServerlet(listeners, null, null, true);
    }
    
    public static OpflowServerlet createServerlet(OpflowServerlet.ListenerDescriptor listeners,
            String propFile) throws OpflowBootstrapException {
        return createServerlet(listeners, null, propFile, true);
    }
    
    public static OpflowServerlet createServerlet(OpflowServerlet.ListenerDescriptor listeners,
            Map<String, Object> config) throws OpflowBootstrapException {
        return createServerlet(listeners, config, null, false);
    }
    
    public static OpflowServerlet createServerlet(OpflowServerlet.ListenerDescriptor listeners,
            Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowServerlet(listeners, new OpflowConfig.LoaderImplServerlet(config, configFile, useDefaultFile));
    }
    
    public static OpflowRpcAmqpMaster createAmqpMaster() throws OpflowBootstrapException {
        return createAmqpMaster(null, null, true);
    }
    
    public static OpflowRpcAmqpMaster createAmqpMaster(String configFile) throws OpflowBootstrapException {
        return createAmqpMaster(null, configFile, true);
    }
    
    public static OpflowRpcAmqpMaster createAmqpMaster(Map<String, Object> config) throws OpflowBootstrapException {
        return createAmqpMaster(config, null, false);
    }
    
    public static OpflowRpcAmqpMaster createAmqpMaster(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowRpcAmqpMaster((new OpflowConfig.LoaderImplRpcAmqpMaster(config, configFile, useDefaultFile)).loadConfiguration());
    }
    
    public static OpflowRpcAmqpWorker createAmqpWorker() throws OpflowBootstrapException {
        return createAmqpWorker(null, null, true);
    }
    
    public static OpflowRpcAmqpWorker createAmqpWorker(String configFile) throws OpflowBootstrapException {
        return createAmqpWorker(null, configFile, true);
    }
    
    public static OpflowRpcAmqpWorker createAmqpWorker(Map<String, Object> config) throws OpflowBootstrapException {
        return createAmqpWorker(config, null, false);
    }
    
    public static OpflowRpcAmqpWorker createAmqpWorker(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowRpcAmqpWorker((new OpflowConfig.LoaderImplRpcAmqpWorker(config, configFile, useDefaultFile)).loadConfiguration());
    }
    
    public static OpflowPubsubHandler createPubsubHandler() throws OpflowBootstrapException {
        return createPubsubHandler(null, null, true);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(String propFile) throws OpflowBootstrapException {
        return createPubsubHandler(null, propFile, true);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(Map<String, Object> config) throws OpflowBootstrapException {
        return createPubsubHandler(config, null, false);
    }
    
    public static OpflowPubsubHandler createPubsubHandler(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowPubsubHandler((new OpflowConfig.LoaderImplPubsubHandler(config, configFile, useDefaultFile)).loadConfiguration());
    }
}
