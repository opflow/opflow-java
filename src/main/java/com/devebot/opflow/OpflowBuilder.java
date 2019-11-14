package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.parser.ParserException;
import org.yaml.snakeyaml.scanner.ScannerException;

/**
 *
 * @author drupalex
 */
public class OpflowBuilder {
    public final static String DEFAULT_CONFIGURATION_KEY = "opflow.configuration";
    public final static String DEFAULT_CONFIGURATION_ENV = "OPFLOW_CONFIGURATION";
    public final static String DEFAULT_CONFIGURATION_FILE = "opflow.properties";
    
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
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowRpcMaster with properties file")
                .stringify());
        
        config = loadConfiguration(config, configFile, useDefaultFile);
        Map<String, Object> params = new HashMap<>();
        
        String[] handlerPath = new String[] {"opflow", "master"};
        extractEngineParameters(params, config, handlerPath);
        Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
        
        params.put("responseName", handlerNode.get("responseName"));
        params.put("responseDurable", handlerNode.get("responseDurable"));
        params.put("responseExclusive", handlerNode.get("responseExclusive"));
        params.put("responseAutoDelete", handlerNode.get("responseAutoDelete"));
        params.put("responseNamePostfixed", handlerNode.get("responseNamePostfixed"));
        
        transformParameters(params);
        
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
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
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowRpcWorker with properties file")
                .stringify());
        
        config = loadConfiguration(config, configFile, useDefaultFile);
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
        
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
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
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", configFile)
                .text("Create new OpflowPubsubHandler with properties file")
                .stringify());
        
        config = loadConfiguration(config, configFile, useDefaultFile);
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
        
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
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
        config = loadConfiguration(config, configFile, useDefaultFile);
        
        Map<String, Object> params = new HashMap<>();
        String[] componentNames = new String[] {"configurer", "rpcMaster", "publisher"};
        String[] componentPath = new String[] {"opflow", "commander", ""};
        for(String componentName:componentNames) {
            componentPath[2] = componentName;
            Map<String, Object> componentCfg = new HashMap<>();
            extractEngineParameters(componentCfg, config, componentPath);
            Map<String, Object> componentNode = getChildMapByPath(config, componentPath);
            componentCfg.put("enabled", componentNode.get("enabled"));
            if ("rpcMaster".equals(componentName)) {
                componentCfg.put("responseName", componentNode.get("responseName"));
                componentCfg.put("responseDurable", componentNode.get("responseDurable"));
                componentCfg.put("responseExclusive", componentNode.get("responseExclusive"));
                componentCfg.put("responseAutoDelete", componentNode.get("responseAutoDelete"));
                componentCfg.put("responseNamePostfixed", componentNode.get("responseNamePostfixed"));
                componentCfg.put("monitorId", componentNode.get("monitorId"));
                componentCfg.put("monitorEnabled", componentNode.get("monitorEnabled"));
                componentCfg.put("monitorInterval", componentNode.get("monitorInterval"));
                componentCfg.put("monitorTimeout", componentNode.get("monitorTimeout"));
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
        config = loadConfiguration(config, configFile, useDefaultFile);
        
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
    
    public static Properties loadProperties() throws OpflowBootstrapException {
        Properties props = new Properties();
        URL url = OpflowUtil.getResource(DEFAULT_CONFIGURATION_FILE);
        try {
            if (url != null) props.load(url.openStream());
        } catch (IOException exception) {
            throw new OpflowBootstrapException(exception);
        }
        return props;
    }
    
    public static Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
        return loadConfiguration(null, null, true);
    }
    
    public static Map<String, Object> loadConfiguration(String configFile) throws OpflowBootstrapException {
        return loadConfiguration(null, configFile, configFile == null);
    }
    
    public static Map<String, Object> loadConfiguration(Map<String, Object> config, String configFile) throws OpflowBootstrapException {
        return loadConfiguration(config, configFile, configFile == null && config == null);
    }
    
    public static Map<String, Object> loadConfiguration(Map<String, Object> config, String configFile, boolean useDefaultFile) throws OpflowBootstrapException {
        try {
            Yaml yaml = new Yaml();
            if (config == null) {
                config = new HashMap<>();
            }
            if (configFile != null || useDefaultFile) {
                URL url = getConfigurationUrl(configFile);
                if (url != null) {
                    String ext = getConfigurationExtension(url);
                    if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                            .put("configFile", url.getFile())
                            .put("extension", ext)
                            .text("load configuration file")
                            .stringify());
                    if ("yml".equals(ext) || "yaml".equals(ext)) {
                        Map<String,Object> yamlConfig = (Map<String,Object>)yaml.load(url.openStream());
                        mergeConfiguration(config, yamlConfig);
                    } else if ("properties".equals(ext)) {
                        Properties props = new Properties();
                        props.load(url.openStream());
                        mergeConfiguration(config, props);
                    }
                } else {
                    configFile = (configFile != null) ? configFile : DEFAULT_CONFIGURATION_FILE;
                    throw new FileNotFoundException("configuration file '" + configFile + "' not found");
                }
            }
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                    .put("YAML", OpflowJsontool.toString(config))
                    .text("loaded properties content")
                    .stringify());
            return config;
        } catch (IOException exception) {
            throw new OpflowBootstrapException(exception);
        } catch (ParserException | ScannerException exception) {
            throw new OpflowBootstrapException(exception);
        } catch (MarkedYAMLException exception) {
            throw new OpflowBootstrapException(exception);
        }
    }
    
    private static URL getConfigurationUrl(String configFile) {
        URL url;
        String cfgFromSystem = OpflowUtil.getSystemProperty(DEFAULT_CONFIGURATION_KEY, null);
        if (cfgFromSystem == null) {
            cfgFromSystem = configFile;
        }
        if (cfgFromSystem == null) {
            cfgFromSystem = OpflowUtil.getEnvironVariable(DEFAULT_CONFIGURATION_ENV, null);
        }
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", cfgFromSystem)
                .text("detected configuration file")
                .stringify());
        if (cfgFromSystem == null) {
            url = OpflowUtil.getResource(DEFAULT_CONFIGURATION_FILE);
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", url)
                .text("default configuration url")
                .stringify());
        } else {
            try {
                url = new URL(cfgFromSystem);
            } catch (MalformedURLException ex) {
                // woa, the cfgFromSystem string is not a URL,
                // attempt to get the resource from the class path
                url = OpflowUtil.getResource(cfgFromSystem);
            }
        }
        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
                .put("configFile", url)
                .text("final configuration url")
                .stringify());
        return url;
    }
    
    public static String getConfigurationExtension(URL url) {
        String filename = url.getFile();
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
    
    public static Map<String, Object> mergeConfiguration(Map<String, Object> target, Map<String, Object> source) {
        if (target == null) target = new HashMap<>();
        if (source == null) return target;
        for (String key : source.keySet()) {
            if (source.get(key) instanceof Map && target.get(key) instanceof Map) {
                Map<String, Object> targetChild = (Map<String, Object>) target.get(key);
                Map<String, Object> sourceChild = (Map<String, Object>) source.get(key);
                target.put(key, mergeConfiguration(targetChild, sourceChild));
            } else {
                target.put(key, source.get(key));
            }
        }
        return target;
    }
    
    public static Map<String, Object> mergeConfiguration(Map<String, Object> target, Properties props) {
        if (target == null) target = new HashMap<>();
        if (props == null) return target;
        Set<Object> keys = props.keySet();
        for(Object keyo:keys) {
            String key = (String) keyo;
            String[] path = key.split("\\.");
            if (path.length > 0) {
                Map<String, Object> current = target;
                for(int i=0; i<(path.length - 1); i++) {
                    if (!current.containsKey(path[i]) || !(current.get(path[i]) instanceof Map)) {
                        current.put(path[i], new HashMap<String, Object>());
                    }
                    current = (Map<String, Object>)current.get(path[i]);
                }
                current.put(path[path.length - 1], props.getProperty(key));
            }
        }
        return target;
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
        Object sourceObject = traverseMapByPath(source, path);
        if(sourceObject != null && sourceObject instanceof Map) {
            return (Map<String, Object>) sourceObject;
        }
        Map<String, Object> blank = new HashMap<>();
        blank.put("enabled", false);
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
        "responseDurable", "responseExclusive", "responseAutoDelete", "responseNamePostfixed"
    };
    
    private static final String[] STRING_ARRAY_FIELDS = new String[] { "otherKeys" };
    
    private static final String[] INTEGER_FIELDS = new String[] {
        "port", "channelMax", "frameMax", "heartbeat", "networkRecoveryInterval", 
        "prefetch", "subscriberLimit", "redeliveredLimit", "monitorInterval", "threadPoolSize"
    };
    
    private static final String[] LONGINT_FIELDS = new String[] {
        "monitorTimeout"
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
                        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
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
                        if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(LOG_TRACER
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
