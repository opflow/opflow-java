package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.Map;

/**
 *
 * @author drupalex
 */
public class OpflowBuilder {
    
    public static OpflowRpcMaster createRpcMaster() throws OpflowBootstrapException {
        return createRpcMaster(null, null, true);
    }
    
    public static OpflowRpcMaster createRpcMaster(String configFile) throws OpflowBootstrapException {
        return createRpcMaster(null, configFile, true);
    }
    
    public static OpflowRpcMaster createRpcMaster(Map<String, Object> config) throws OpflowBootstrapException {
        return createRpcMaster(config, null, false);
    }
    
    public static OpflowRpcMaster createRpcMaster(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowRpcMaster((new OpflowConfig.LoaderImplRpcMaster(config, configFile, useDefaultFile)).loadConfiguration());
    }
    
    public static OpflowRpcWorker createRpcWorker() throws OpflowBootstrapException {
        return createRpcWorker(null, null, true);
    }
    
    public static OpflowRpcWorker createRpcWorker(String configFile) throws OpflowBootstrapException {
        return createRpcWorker(null, configFile, true);
    }
    
    public static OpflowRpcWorker createRpcWorker(Map<String, Object> config) throws OpflowBootstrapException {
        return createRpcWorker(config, null, false);
    }
    
    public static OpflowRpcWorker createRpcWorker(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        return new OpflowRpcWorker((new OpflowConfig.LoaderImplRpcWorker(config, configFile, useDefaultFile)).loadConfiguration());
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
}
