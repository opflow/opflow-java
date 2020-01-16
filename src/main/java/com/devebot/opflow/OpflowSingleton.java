package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowSingleton {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowSingleton.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();

    private static final Map<String, OpflowCommander> COMMANDERS;
    private static final Map<String, OpflowServerlet> SERVERLETS;
    private static final Map<String, OpflowPubsubHandler> PUBSUB_HANDLERS;
    private static final Map<String, OpflowRpcMaster> RPC_MASTERS;
    private static final Map<String, OpflowRpcWorker> RPC_WORKERS;

    static {
        if (LOG_TRACER.ready(LOG, "debug")) LOG.debug(LOG_TRACER
                            .text("Create the lookup tables for Opflow Handlers")
                            .stringify());
        COMMANDERS = new ConcurrentHashMap<>();
        SERVERLETS = new ConcurrentHashMap<>();
        PUBSUB_HANDLERS = new ConcurrentHashMap<>();
        RPC_MASTERS = new ConcurrentHashMap<>();
        RPC_WORKERS = new ConcurrentHashMap<>();
    }

    public static OpflowCommander assertCommander(String configFile) throws OpflowBootstrapException {
        return assertHandler(configFile, OpflowCommander.class, COMMANDERS);
    }

    public static OpflowServerlet assertServerlet(String configFile) throws OpflowBootstrapException {
        return assertHandler(configFile, OpflowServerlet.class, SERVERLETS);
    }

    public static OpflowPubsubHandler assertPubsubHandler(String configFile) throws OpflowBootstrapException {
        return assertHandler(configFile, OpflowPubsubHandler.class, PUBSUB_HANDLERS);
    }

    public static OpflowRpcMaster assertRpcMaster(String configFile) throws OpflowBootstrapException {
        return assertHandler(configFile, OpflowRpcMaster.class, RPC_MASTERS);
    }
    
    public static OpflowRpcWorker assertRpcWorker(String configFile) throws OpflowBootstrapException {
        return assertHandler(configFile, OpflowRpcWorker.class, RPC_WORKERS);
    }
    
    private static <T> T assertHandler(String configFile, Class<T> clazz, Map<String, T> map) throws OpflowBootstrapException {
        String uniqCode = genUniqCode(configFile);
        if (!map.containsKey(uniqCode)) {
            synchronized(map) {
                if (!map.containsKey(uniqCode)) {
                    if (LOG_TRACER.ready(LOG, "debug")) LOG.debug(LOG_TRACER
                            .tags(clazz.getCanonicalName())
                            .put("handlerName", clazz.getCanonicalName())
                            .text("A handler[${handlerName}] is created")
                            .stringify());
                    if (OpflowCommander.class.equals(clazz)) {
                        map.put(uniqCode, (T) OpflowBuilder.createCommander(configFile));
                    }
                    if (OpflowServerlet.class.equals(clazz)) {
                        map.put(uniqCode, (T) OpflowBuilder.createServerlet(configFile));
                    }
                    if (OpflowPubsubHandler.class.equals(clazz)) {
                        map.put(uniqCode, (T) OpflowBuilder.createPubsubHandler(configFile));
                    }
                    if (OpflowRpcMaster.class.equals(clazz)) {
                        map.put(uniqCode, (T) OpflowBuilder.createRpcMaster(configFile));
                    }
                    if (OpflowRpcWorker.class.equals(clazz)) {
                        map.put(uniqCode, (T) OpflowBuilder.createRpcWorker(configFile));
                    }
                }
            }
        }
        return map.get(uniqCode);
    }
    
    private static String genUniqCode(String configFile) {
        if (configFile == null) {
            return "_";
        }
        return configFile;
    }
}
