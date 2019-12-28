package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowBuilder {

    private final static Logger LOG = LoggerFactory.getLogger(OpflowBuilder.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();
    
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
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowRpcMaster with properties file")
                .stringify());
        
        config = OpflowConfigLoader.loadConfiguration(config, configFile, useDefaultFile);
        Map<String, Object> params = new HashMap<>();
        
        String[] handlerPath = new String[] {"opflow", "master"};
        extractEngineParameters(params, config, handlerPath);
        Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
        
        params.put("responseName", handlerNode.get("responseName"));
        params.put("responseQueueSuffix", handlerNode.get("responseQueueSuffix"));
        params.put("responseDurable", handlerNode.get("responseDurable"));
        params.put("responseExclusive", handlerNode.get("responseExclusive"));
        params.put("responseAutoDelete", handlerNode.get("responseAutoDelete"));
        
        transformParameters(params);
        
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .text("OpflowRpcMaster has been created successfully")
                .stringify());
        
        return new OpflowRpcMaster(params);
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
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowRpcWorker with properties file")
                .stringify());
        
        config = OpflowConfigLoader.loadConfiguration(config, configFile, useDefaultFile);
        Map<String, Object> params = new HashMap<>();
        
        String[] handlerPath = new String[] {"opflow", "worker"};
        extractEngineParameters(params, config, handlerPath);
        Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
        Map<String, Object> opflowNode = getChildMapByPath(config, new String[] {"opflow"});
        
        if (handlerNode.get("operatorName") != null) {
            params.put("operatorName", handlerNode.get("operatorName"));
        } else {
            params.put("operatorName", opflowNode.get("queueName"));
        }
        
        params.put("responseName", handlerNode.get("responseName"));
        params.put("prefetch", handlerNode.get("prefetch"));
        
        transformParameters(params);
        
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .text("OpflowRpcWorker has been created successfully")
                .stringify());
        
        return new OpflowRpcWorker(params);
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
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowPubsubHandler with properties file")
                .stringify());
        
        config = OpflowConfigLoader.loadConfiguration(config, configFile, useDefaultFile);
        Map<String, Object> params = new HashMap<>();
        
        String[] handlerPath = new String[] {"opflow", "pubsub"};
        extractEngineParameters(params, config, handlerPath);
        Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
        Map<String, Object> opflowNode = getChildMapByPath(config, new String[] {"opflow"});
        
        if (handlerNode.get("subscriberName") != null) {
            params.put("subscriberName", handlerNode.get("subscriberName"));
        } else {
            params.put("subscriberName", opflowNode.get("queueName"));
        }
        
        params.put("recyclebinName", handlerNode.get("recyclebinName"));
        params.put("prefetch", handlerNode.get("prefetch"));
        params.put("subscriberLimit", handlerNode.get("subscriberLimit"));
        params.put("redeliveredLimit", handlerNode.get("redeliveredLimit"));
        
        transformParameters(params);
        
        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                .text("OpflowPubsubHandler has been created successfully")
                .stringify());
        
        return new OpflowPubsubHandler(params);
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
        config = OpflowConfigLoader.loadConfiguration(config, configFile, useDefaultFile);
        
        Map<String, Object> params = new HashMap<>();
        String[] componentPath = new String[] {"opflow", "commander", ""};
        for(String componentName:OpflowCommander.ALL_BEAN_NAMES) {
            componentPath[2] = componentName;
            Map<String, Object> componentCfg = new HashMap<>();
            Map<String, Object> componentNode;
            if (OpflowCommander.SERVICE_BEAN_NAMES.contains(componentName)) {
                extractEngineParameters(componentCfg, config, componentPath);
                componentNode = getChildMapByPath(config, componentPath);
            } else {
                componentNode = getChildMapByPath(config, componentPath, false);
            }
            componentCfg.put("enabled", componentNode.get("enabled"));
            if ("rpcMaster".equals(componentName)) {
                componentCfg.put("expiration", componentNode.get("expiration"));
                componentCfg.put("responseName", componentNode.get("responseName"));
                componentCfg.put("responseDurable", componentNode.get("responseDurable"));
                componentCfg.put("responseExclusive", componentNode.get("responseExclusive"));
                componentCfg.put("responseAutoDelete", componentNode.get("responseAutoDelete"));
                componentCfg.put("responseQueueSuffix", componentNode.get("responseQueueSuffix"));
                componentCfg.put("monitorId", componentNode.get("monitorId"));
                componentCfg.put("monitorEnabled", componentNode.get("monitorEnabled"));
                componentCfg.put("monitorInterval", componentNode.get("monitorInterval"));
                componentCfg.put("monitorTimeout", componentNode.get("monitorTimeout"));
            }
            if ("rpcWatcher".equals(componentName)) {
                componentCfg.put("interval", componentNode.get("interval"));
            }
            if ("infoProvider".equals(componentName)) {
                componentCfg.put("host", componentNode.get("host"));
                componentCfg.put("port", componentNode.get("port"));
            }
            transformParameters(componentCfg);
            params.put(componentName, componentCfg);
        }
        
        return new OpflowCommander(params);
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
        config = OpflowConfigLoader.loadConfiguration(config, configFile, useDefaultFile);
        
        Map<String, Object> params = new HashMap<>();
        String[] componentNames = new String[] {"configurer", "rpcWorker", "subscriber"};
        String[] componentPath = new String[] {"opflow", "serverlet", ""};
        for(String componentName:componentNames) {
            componentPath[2] = componentName;
            Map<String, Object> componentCfg = new HashMap<>();
            extractEngineParameters(componentCfg, config, componentPath);
            Map<String, Object> componentNode = getChildMapByPath(config, componentPath);
            componentCfg.put("enabled", componentNode.get("enabled"));
            if ("rpcWorker".equals(componentName)) {
                componentCfg.put("operatorName", componentNode.get("operatorName"));
                componentCfg.put("responseName", componentNode.get("responseName"));
            }
            if ("subscriber".equals(componentName)) {
                componentCfg.put("subscriberName", componentNode.get("subscriberName"));
                componentCfg.put("recyclebinName", componentNode.get("recyclebinName"));
            }
            transformParameters(componentCfg);
            params.put(componentName, componentCfg);
        }
        
        return new OpflowServerlet(listeners, params);
    }
    
    private static String getPropertyAsString(Properties prop) {
        StringWriter writer = new StringWriter();
        prop.list(new PrintWriter(writer));
        return writer.getBuffer().toString();
    }
    
    private static Object traverseMapByPath(Map<String, Object> source, String[] path) {
        Map<String, Object> point = source;
        Object value = null;
        for(String node : path) {
            if (point == null) return null;
            value = point.get(node);
            point = (value instanceof Map) ? (Map<String, Object>) value : null;
        }
        return value;
    }
    
    private static Map<String, Object> getChildMapByPath(Map<String, Object> source, String[] path) {
        return getChildMapByPath(source, path, true);
    }
    
    private static Map<String, Object> getChildMapByPath(Map<String, Object> source, String[] path, boolean disableByEmpty) {
        Object sourceObject = traverseMapByPath(source, path);
        if(sourceObject != null && sourceObject instanceof Map) {
            return (Map<String, Object>) sourceObject;
        }
        Map<String, Object> blank = new HashMap<>();
        if (disableByEmpty) {
            blank.put("enabled", false);
        }
        return blank;
    }
    
    private static void extractEngineParameters(Map<String, Object> target, Map<String, Object> source, String[] path) {
        List<Map<String, Object>> maps = new ArrayList<>(path.length);
        for(String node:path) {
            if (source.get(node) != null && source.get(node) instanceof Map) {
                source = (Map<String, Object>)source.get(node);
                maps.add(0, source);
            } else {
                break;
            }
        }
        for(String field: OpflowEngine.PARAMETER_NAMES) {
            Iterator<Map<String, Object>> iter = maps.iterator();
            while(iter.hasNext() && !target.containsKey(field)) {
                Map<String, Object> map = iter.next();
                if (map.containsKey(field)) {
                    target.put(field, map.get(field));
                }
            }
        }
    }
    
    private static final String[] BOOLEAN_FIELDS = new String[] {
        "enabled", "verbose", "automaticRecoveryEnabled", "topologyRecoveryEnabled", "monitorEnabled",
        "responseDurable", "responseExclusive", "responseAutoDelete"
    };

    private static final String[] STRING_FIELDS = new String[] {
        "responseQueueSuffix"
    };
    
    private static final String[] STRING_ARRAY_FIELDS = new String[] { "otherKeys" };
    
    private static final String[] INTEGER_FIELDS = new String[] {
        "port", "channelMax", "frameMax", "heartbeat", "networkRecoveryInterval", 
        "prefetch", "subscriberLimit", "redeliveredLimit", "monitorInterval", "threadPoolSize"
    };
    
    private static final String[] LONGINT_FIELDS = new String[] {
        "expiration", "interval", "monitorTimeout"
    };
    
    private static void transformParameters(Map<String, Object> params) {
        for(String key: params.keySet()) {
            if (OpflowUtil.arrayContains(BOOLEAN_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    params.put(key, Boolean.parseBoolean(params.get(key).toString()));
                }
            }
            if (OpflowUtil.arrayContains(STRING_ARRAY_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    params.put(key, OpflowUtil.splitByComma((String)params.get(key)));
                }
            }
            if (OpflowUtil.arrayContains(INTEGER_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    try {
                        params.put(key, Integer.parseInt(params.get(key).toString()));
                    } catch (NumberFormatException nfe) {
                        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                                .put("fieldName", key)
                                .text("transformParameters() - field is not an integer")
                                .stringify());
                        params.put(key, null);
                    }
                }
            }
            if (OpflowUtil.arrayContains(LONGINT_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    try {
                        params.put(key, Long.parseLong(params.get(key).toString()));
                    } catch (NumberFormatException nfe) {
                        if (LOG_TRACER.ready(LOG, "trace")) LOG.trace(LOG_TRACER
                                .put("fieldName", key)
                                .text("transformParameters() - field is not a longint")
                                .stringify());
                        params.put(key, null);
                    }
                }
            }
        }
    }
}
