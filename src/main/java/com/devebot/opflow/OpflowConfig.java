package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowCollectionUtil;
import com.devebot.opflow.supports.OpflowEnvTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowStringUtil;
import org.yaml.snakeyaml.nostro.Yaml;
import org.yaml.snakeyaml.nostro.error.MarkedYAMLException;
import org.yaml.snakeyaml.nostro.parser.ParserException;
import org.yaml.snakeyaml.nostro.scanner.ScannerException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowConfig {
    public final static String DEFAULT_CONFIGURATION_KEY = "opflow.configuration";
    public final static String DEFAULT_CONFIGURATION_ENV = "OPFLOW_CONFIGURATION";
    public final static String DEFAULT_CONFIGURATION_FILE = "opflow.properties";

    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;
    private final static Logger LOG = LoggerFactory.getLogger(OpflowConfig.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();
    
    public interface Loader {
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException;
    }
    
    public static class LoaderImplRpcAmqpMaster implements OpflowConfig.Loader {
        
        private Map<String, Object> config;
        private final String configFile;
        private final boolean useDefaultFile;
        
        public LoaderImplRpcAmqpMaster(Map<String, Object> config, String configFile, boolean useDefaultFile) {
            this.config = config;
            this.configFile = configFile;
            this.useDefaultFile = useDefaultFile;
        }
        
        @Override
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
            config = OpflowConfig.loadConfiguration(config, configFile, useDefaultFile);
            Map<String, Object> params = new HashMap<>();

            String[] handlerPath = new String[] {OpflowConstant.FRAMEWORK_ID, "master"};
            extractEngineParameters(params, config, handlerPath);
            Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);

            OpflowUtil.copyParameters(params, handlerNode, new String[] {
                OpflowConstant.OPFLOW_COMMON_AUTORUN,
                OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME,
                OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_TYPE,
                OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE,
                OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY,
                OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE,
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME,
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_SUFFIX,
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE,
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE,
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE,
                OpflowConstant.OPFLOW_RESPONSE_PREFETCH_COUNT,
            });

            transformParameters(params);
            return params;
        }
    }
    
    public static class LoaderImplRpcAmqpWorker implements OpflowConfig.Loader {
        
        private Map<String, Object> config;
        private final String configFile;
        private final boolean useDefaultFile;
        
        public LoaderImplRpcAmqpWorker(Map<String, Object> config, String configFile, boolean useDefaultFile) {
            this.config = config;
            this.configFile = configFile;
            this.useDefaultFile = useDefaultFile;
        }
        
        @Override
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
            config = OpflowConfig.loadConfiguration(config, configFile, useDefaultFile);
            Map<String, Object> params = new HashMap<>();

            String[] handlerPath = new String[] {OpflowConstant.FRAMEWORK_ID, "worker"};
            extractEngineParameters(params, config, handlerPath);
            Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
            Map<String, Object> opflowNode = getChildMapByPath(config, new String[] {OpflowConstant.FRAMEWORK_ID});

            OpflowUtil.copyParameters(params, handlerNode, new String[] {
                OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME,
                OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME,
                OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY,
                OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE,
                OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE,
                OpflowConstant.OPFLOW_INCOMING_PREFETCH_COUNT,
                OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_NAME,
                OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_TYPE,
                OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_DURABLE,
                OpflowConstant.OPFLOW_OUTGOING_ROUTING_KEY,
            });

            if (handlerNode.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME) == null) {
                params.put(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME, opflowNode.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME));
            }
            
            if (handlerNode.get(OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_NAME) == null) {
                params.put(OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_NAME, opflowNode.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME));
            }

            transformParameters(params);
            return params;
        }
    }
    
    public static class LoaderImplPubsubHandler implements OpflowConfig.Loader {
        
        private Map<String, Object> config;
        private final String configFile;
        private final boolean useDefaultFile;
        
        public LoaderImplPubsubHandler(Map<String, Object> config, String configFile, boolean useDefaultFile) {
            this.config = config;
            this.configFile = configFile;
            this.useDefaultFile = useDefaultFile;
        }
        
        @Override
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
            config = OpflowConfig.loadConfiguration(config, configFile, useDefaultFile);
            Map<String, Object> params = new HashMap<>();

            String[] handlerPath = new String[] {OpflowConstant.FRAMEWORK_ID, "pubsub"};
            extractEngineParameters(params, config, handlerPath);
            Map<String, Object> handlerNode = getChildMapByPath(config, handlerPath);
            Map<String, Object> opflowNode = getChildMapByPath(config, new String[] {OpflowConstant.FRAMEWORK_ID});

            OpflowUtil.copyParameters(params, handlerNode, new String[] {
                OpflowConstant.OPFLOW_COMMON_AUTORUN,
                OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME,
                OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_TYPE,
                OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_DURABLE,
                OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY,
                OpflowConstant.OPFLOW_PUBSUB_BINDING_KEYS,
                OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME,
                OpflowConstant.OPFLOW_PUBSUB_CONSUMER_LIMIT,
                OpflowConstant.OPFLOW_PUBSUB_PREFETCH_COUNT,
                OpflowConstant.OPFLOW_PUBSUB_REDELIVERED_LIMIT,
                OpflowConstant.OPFLOW_PUBSUB_TRASH_NAME,
            });

            if (handlerNode.get(OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME) == null) {
                params.put(OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME, opflowNode.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME));
            }

            transformParameters(params);
            return params;
        }
    }
    
    public static class LoaderImplCommander implements OpflowConfig.Loader {
        
        private Map<String, Object> config;
        private final String configFile;
        private final boolean useDefaultFile;
        
        public LoaderImplCommander(Map<String, Object> config, String configFile, boolean useDefaultFile) {
            this.config = config;
            this.configFile = configFile;
            this.useDefaultFile = useDefaultFile;
        }
        
        @Override
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
            config = OpflowConfig.loadConfiguration(config, configFile, useDefaultFile);
            Map<String, Object> params = new HashMap<>();

            // extract the top-level configuration
            Map<String, Object> componentRoot = getChildMapByPath(config, new String[] {OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_COMMANDER});
            OpflowUtil.copyParameters(params, componentRoot, new String[] {
                OpflowConstant.OPFLOW_COMMON_STRICT,
                OpflowConstant.OPFLOW_COMMON_SERVICE_NAME,
            });
            
            if (componentRoot.containsKey(OpflowConstant.COMP_CFG_AMQP_MASTER)) {
                if (componentRoot.containsKey(OpflowConstant.COMP_RPC_AMQP_MASTER)) {
                    throw new OpflowBootstrapException(MessageFormat.format("Please convert the section [{0}] to [{1}]", new Object[] {
                        OpflowConstant.COMP_CFG_AMQP_MASTER,
                        OpflowConstant.COMP_RPC_AMQP_MASTER,
                    }));
                }
                renameField(componentRoot, OpflowConstant.COMP_CFG_AMQP_MASTER, OpflowConstant.COMP_RPC_AMQP_MASTER);
            }
            
            // extract the child-level configuration
            String[] componentPath = new String[] {OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_COMMANDER, ""};
            for(String componentName : OpflowCommander.ALL_BEAN_NAMES) {
                componentPath[2] = componentName;
                Map<String, Object> componentCfg = new HashMap<>();
                Map<String, Object> componentNode;
                if (OpflowCommander.SERVICE_BEAN_NAMES.contains(componentName)) {
                    extractEngineParameters(componentCfg, config, componentPath);
                    componentNode = getChildMapByPath(config, componentPath);
                } else {
                    componentNode = getChildMapByPath(config, componentPath, false);
                }
                switch (componentName) {
                    case OpflowConstant.COMP_DISCOVERY_CLIENT:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_DISCOVERY_CLIENT_AGENT_HOSTS,
                        });
                        break;
                    case OpflowConstant.COMP_REQ_EXTRACTOR:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_REQ_EXTRACTOR_CLASS_NAME,
                            OpflowConstant.OPFLOW_REQ_EXTRACTOR_METHOD_NAME,
                            OpflowConstant.OPFLOW_REQ_EXTRACTOR_AUTO_UUID
                        });
                        break;
                    case OpflowConstant.COMP_RESTRICTOR:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_RESTRICT_PAUSE_ENABLED,
                            OpflowConstant.OPFLOW_RESTRICT_PAUSE_TIMEOUT,
                            OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_ENABLED,
                            OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_PERMITS,
                            OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_TIMEOUT
                        });
                        break;
                    case OpflowConstant.COMP_PUBLISHER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_TYPE,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_DURABLE,
                            OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY,
                            OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME,
                        });
                        break;
                    case OpflowConstant.COMP_RPC_AMQP_MASTER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.AMQP_PARAM_MESSAGE_TTL,
                            OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME,
                            OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_TYPE,
                            OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE,
                            OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY,
                            OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_SUFFIX,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE,
                            OpflowConstant.OPFLOW_RESPONSE_PREFETCH_COUNT,
                            OpflowConstant.OPFLOW_RPC_MONITOR_ID,
                            OpflowConstant.OPFLOW_RPC_MONITOR_ENABLED,
                            OpflowConstant.OPFLOW_RPC_MONITOR_INTERVAL,
                            OpflowConstant.OPFLOW_RPC_MONITOR_TIMEOUT,
                        });
                        break;
                    case OpflowConstant.COMP_RPC_HTTP_MASTER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                        });
                        break;
                    case OpflowConstant.COMP_RPC_OBSERVER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_ENABLED,
                            OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_TYPE,
                            OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_SIZE,
                            OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_ENABLED,
                            OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_TIME_DELAY,
                        });
                        break;
                    case OpflowConstant.COMP_GARBAGE_COLLECTOR:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_INTERVAL
                        });
                        break;
                    case OpflowConstant.COMP_RPC_WATCHER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_INTERVAL
                        });
                        break;
                    case OpflowConstant.COMP_SPEED_METER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ACTIVE,
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_INTERVAL,
                            OpflowConstant.OPFLOW_COMMON_LENGTH
                        });
                        break;
                    case OpflowConstant.COMP_PROM_EXPORTER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_HOST,
                            OpflowConstant.OPFLOW_COMMON_PORTS
                        });
                        break;
                    case OpflowConstant.COMP_REST_SERVER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_HOST,
                            OpflowConstant.OPFLOW_COMMON_PORTS,
                            OpflowConstant.OPFLOW_COMMON_CREDENTIALS
                        });
                        break;
                }
                transformParameters(componentCfg);
                params.put(componentName, componentCfg);
            }
            
            // connector hashMap
            Map<String, Object> connectorHash = getChildMapByPath(config, new String[] {OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_CONNECTOR}, false);
            
            // validate the connector names and connector config fields
            if (connectorHash != null) {
                for (String connectorName : connectorHash.keySet()) {
                    Map<String, Object> connectorCfg = OpflowUtil.getChildMap(connectorHash, connectorName);
                    if (connectorCfg != null) {
                        for (String amqpCompName : OpflowCommander.SERVICE_BEAN_NAMES) {
                            Map<String, Object> componentCfg = OpflowUtil.getChildMap(connectorCfg, amqpCompName);
                            if (componentCfg != null) {
                                OpflowUtil.copyParameters(componentCfg, getChildMapByPath(params, new String[] {
                                    amqpCompName
                                }), OpflowEngine.PARAMETER_NAMES, false);
                                OpflowUtil.copyParameters(componentCfg, getChildMapByPath(params, new String[] {
                                    amqpCompName
                                }), OpflowEngine.SHARED_DEFAULT_PARAMS, false);
                            }
                        }
                    }
                }
            }
            
            // assign the connector hash map to the params
            params.put(OpflowConstant.COMP_CONNECTOR, connectorHash);
            
            // System.out.println("CONFIG: " + OpflowJsonTool.toString(params, true));
            
            // return the connection parameters
            return params;
        }
    }
    
    public static class LoaderImplServerlet implements OpflowConfig.Loader {
        
        private Map<String, Object> config;
        private final String configFile;
        private final boolean useDefaultFile;
        
        public LoaderImplServerlet(Map<String, Object> config, String configFile, boolean useDefaultFile) {
            this.config = config;
            this.configFile = configFile;
            this.useDefaultFile = useDefaultFile;
        }
        
        @Override
        public Map<String, Object> loadConfiguration() throws OpflowBootstrapException {
            config = OpflowConfig.loadConfiguration(config, configFile, useDefaultFile);
            Map<String, Object> params = new HashMap<>();

            // extract the top-level configuration
            Map<String, Object> componentRoot = getChildMapByPath(config, new String[] {OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_SERVERLET});
            OpflowUtil.copyParameters(params, componentRoot, new String[] {
                OpflowConstant.OPFLOW_COMMON_STRICT,
                OpflowConstant.OPFLOW_COMMON_SERVICE_NAME,
            });
            
            if (componentRoot.containsKey(OpflowConstant.COMP_CFG_AMQP_WORKER)) {
                if (componentRoot.containsKey(OpflowConstant.COMP_RPC_AMQP_WORKER)) {
                    throw new OpflowBootstrapException(MessageFormat.format("Please convert the section [{0}] to [{1}]", new Object[] {
                        OpflowConstant.COMP_CFG_AMQP_WORKER,
                        OpflowConstant.COMP_RPC_AMQP_WORKER,
                    }));
                }
                renameField(componentRoot, OpflowConstant.COMP_CFG_AMQP_WORKER, OpflowConstant.COMP_RPC_AMQP_WORKER);
            }
            
            // extract the child-level configuration
            String[] componentPath = new String[] {OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_SERVERLET, ""};
            for(String componentName : OpflowServerlet.ALL_BEAN_NAMES) {
                componentPath[2] = componentName;
                Map<String, Object> componentCfg = new HashMap<>();
                Map<String, Object> componentNode;
                if (OpflowServerlet.SERVICE_BEAN_NAMES.contains(componentName)) {
                    extractEngineParameters(componentCfg, config, componentPath);
                    componentNode = getChildMapByPath(config, componentPath);
                } else {
                    componentNode = getChildMapByPath(config, componentPath, false);
                }
                switch (componentName) {
                    case OpflowConstant.COMP_DISCOVERY_CLIENT:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_DISCOVERY_CLIENT_AGENT_HOSTS,
                            OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_INTERVAL,
                            OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_TTL,
                        });
                        break;
                    case OpflowConstant.COMP_RPC_AMQP_WORKER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME,
                            OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME,
                            OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY,
                            OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE,
                            OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE,
                            OpflowConstant.OPFLOW_INCOMING_PREFETCH_COUNT,
                            OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_NAME,
                            OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_TYPE,
                            OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_DURABLE,
                            OpflowConstant.OPFLOW_OUTGOING_ROUTING_KEY,
                        });
                        break;
                    case OpflowConstant.COMP_RPC_HTTP_WORKER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_HOST,
                            OpflowConstant.OPFLOW_COMMON_HOSTNAME,
                            OpflowConstant.OPFLOW_COMMON_PORTS,
                        });
                        break;
                    case OpflowConstant.COMP_SUBSCRIBER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_TYPE,
                            OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_DURABLE,
                            OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY,
                            OpflowConstant.OPFLOW_PUBSUB_QUEUE_NAME,
                            OpflowConstant.OPFLOW_PUBSUB_TRASH_NAME
                        });
                        break;
                    case OpflowConstant.COMP_PROM_EXPORTER:
                        OpflowUtil.copyParameters(componentCfg, componentNode, new String[] {
                            OpflowConstant.OPFLOW_COMMON_ENABLED,
                            OpflowConstant.OPFLOW_COMMON_HOST,
                            OpflowConstant.OPFLOW_COMMON_PORTS
                        });
                        break;
                }
                transformParameters(componentCfg);
                params.put(componentName, componentCfg);
            }

            return params;
        }
    }
    
    private static void renameField(Map<String, Object> source, String oldName, String newName) {
        if (!source.containsKey(oldName)) {
            return;
        }
        Map<String, Object> oldMap = OpflowObjectTree.assertChildMap(source, oldName);
        Map<String, Object> newMap = OpflowObjectTree.assertChildMap(source, newName);
        mergeConfiguration(newMap, oldMap);
        source.remove(oldName);
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
            blank.put(OpflowConstant.OPFLOW_COMMON_ENABLED, false);
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
        String[] sharedFields = OpflowCollectionUtil.mergeArrays(OpflowEngine.PARAMETER_NAMES,
                OpflowEngine.SHARED_DEFAULT_PARAMS,
                OpflowEngine.SHARED_PRODUCING_PARAMS);
        for(String field: sharedFields) {
            Iterator<Map<String, Object>> iter = maps.iterator();
            while(iter.hasNext() && !target.containsKey(field)) {
                Map<String, Object> map = iter.next();
                if (map.containsKey(field)) {
                    target.put(field, map.get(field));
                }
            }
        }
    }
    
    private static final String[] BOOLEAN_FIELDS = OpflowCollectionUtil.distinct(new String[] {
        OpflowConstant.OPFLOW_COMMON_ACTIVE,
        OpflowConstant.OPFLOW_COMMON_AUTORUN,
        OpflowConstant.OPFLOW_COMMON_ENABLED,
        OpflowConstant.OPFLOW_COMMON_VERBOSE,
        OpflowConstant.OPFLOW_COMMON_STRICT,
        OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_ENABLED,
        OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_ENABLED,
        OpflowConstant.OPFLOW_RPC_MONITOR_ENABLED,
        OpflowConstant.OPFLOW_RESTRICT_PAUSE_ENABLED,
        OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_ENABLED,
        OpflowConstant.AMQP_CONARG_AUTOMATIC_RECOVERY_ENABLED,
        OpflowConstant.AMQP_CONARG_TOPOLOGY_RECOVERY_ENABLED,
        
        OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE,
        OpflowConstant.OPFLOW_CONSUMING_QUEUE_AUTO_DELETE,
        OpflowConstant.OPFLOW_CONSUMING_QUEUE_DURABLE,
        OpflowConstant.OPFLOW_CONSUMING_QUEUE_EXCLUSIVE,
        
        OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE,
        OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE,
        OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE,
        OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE,
        
        OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE,
        OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE,
        OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE,
        OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_DURABLE,
    });

    private static final String[] STRING_FIELDS = OpflowCollectionUtil.distinct(new String[] {
        OpflowConstant.OPFLOW_RESPONSE_QUEUE_SUFFIX
    });
    
    private static final String[] STRING_ARRAY_FIELDS = OpflowCollectionUtil.distinct(new String[] {
        OpflowConstant.OPFLOW_COMMON_CREDENTIALS,
        OpflowConstant.OPFLOW_DISCOVERY_CLIENT_AGENT_HOSTS,
        OpflowConstant.OPFLOW_CONSUMING_BINDING_KEYS,
        OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS,
    });
    
    private static final String[] INTEGER_FIELDS = OpflowCollectionUtil.distinct(new String[] {
        OpflowConstant.AMQP_CONARG_PORT,
        OpflowConstant.AMQP_CONARG_CONNECTION_TIMEOUT,
        OpflowConstant.AMQP_CONARG_HANDSHAKE_TIMEOUT,
        OpflowConstant.AMQP_CONARG_SHUTDOWN_TIMEOUT,
        OpflowConstant.AMQP_CONARG_REQUESTED_CHANNEL_MAX,
        OpflowConstant.AMQP_CONARG_REQUESTED_FRAME_MAX,
        OpflowConstant.AMQP_CONARG_REQUESTED_HEARTBEAT,
        OpflowConstant.AMQP_CONARG_NETWORK_RECOVERY_INTERVAL,
        OpflowConstant.OPFLOW_COMMON_LENGTH,
        OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_SIZE,
        OpflowConstant.OPFLOW_RPC_MONITOR_INTERVAL,
        OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_PERMITS,
        OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT,
        OpflowConstant.OPFLOW_INCOMING_PREFETCH_COUNT,
        OpflowConstant.OPFLOW_RESPONSE_PREFETCH_COUNT,
        OpflowConstant.OPFLOW_PUBSUB_CONSUMER_LIMIT,
        OpflowConstant.OPFLOW_PUBSUB_REDELIVERED_LIMIT,
        OpflowConstant.AMQP_PARAM_SHARED_THREAD_POOL_SIZE
    });
    
    private static final String[] INTEGER_ARRAY_FIELDS = new String[] { OpflowConstant.OPFLOW_COMMON_PORTS };
    
    private static final String[] INTEGER_RANGE_FIELDS = new String[] { OpflowConstant.OPFLOW_COMMON_PORTS };
    
    private static final String[] LONGINT_FIELDS = OpflowCollectionUtil.distinct(new String[] {
        OpflowConstant.AMQP_PARAM_MESSAGE_TTL,
        OpflowConstant.OPFLOW_COMMON_INTERVAL,
        OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_TIME_DELAY,
        OpflowConstant.OPFLOW_RPC_MONITOR_TIMEOUT,
        OpflowConstant.OPFLOW_RESTRICT_PAUSE_TIMEOUT,
        OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_TIMEOUT,
        OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_INTERVAL,
        OpflowConstant.OPFLOW_DISCOVERY_CLIENT_CHECK_TTL,
    });
    
    private static void transformParameters(Map<String, Object> params) {
        for(String key: params.keySet()) {
            if (OpflowCollectionUtil.arrayContains(BOOLEAN_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    params.put(key, Boolean.parseBoolean(params.get(key).toString()));
                }
            }
            if (OpflowCollectionUtil.arrayContains(STRING_ARRAY_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    params.put(key, OpflowStringUtil.splitByComma((String)params.get(key)));
                }
            }
            if (OpflowCollectionUtil.arrayContains(INTEGER_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    try {
                        params.put(key, Integer.parseInt(params.get(key).toString()));
                    } catch (NumberFormatException nfe) {
                        if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                                .put("fieldName", key)
                                .text("transformParameters() - field is not an integer")
                                .stringify());
                        params.put(key, null);
                    }
                }
            }
            if (OpflowCollectionUtil.arrayContains(INTEGER_ARRAY_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    String intArrayStr = (String) params.get(key);
                    if (OpflowStringUtil.isIntegerArray(intArrayStr)) {
                        params.put(key, OpflowStringUtil.splitByComma(intArrayStr, Integer.class));
                    }
                }
            }
            if (OpflowCollectionUtil.arrayContains(INTEGER_RANGE_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    String intRangeStr = (String) params.get(key);
                    if (OpflowStringUtil.isIntegerRange(intRangeStr)) {
                        Integer[] range = OpflowStringUtil.getIntegerRange(intRangeStr);
                        if (range != null) {
                            params.put(key, new Integer[] { -1, range[0], range[1] });
                        }
                    }
                }
            }
            if (OpflowCollectionUtil.arrayContains(LONGINT_FIELDS, key)) {
                if (params.get(key) instanceof String) {
                    try {
                        params.put(key, Long.parseLong(params.get(key).toString()));
                    } catch (NumberFormatException nfe) {
                        if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                                .put("fieldName", key)
                                .text("transformParameters() - field is not a longint")
                                .stringify());
                        params.put(key, null);
                    }
                }
            }
        }
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
            if (config == null) {
                config = new HashMap<>();
            }
            if (configFile != null || useDefaultFile) {
                URL url = getConfigurationUrl(configFile);
                if (url != null) {
                    String ext = getConfigurationExtension(url);
                    if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                            .put("configFile", url.getFile())
                            .put("extension", ext)
                            .text("load configuration file")
                            .stringify());
                    // load the configuration from YAML/Properties files
                    switch(ext) {
                        case "yaml":
                        case "yml":
                            Yaml yaml = new Yaml();
                            Map<String,Object> yamlConfig = (Map<String,Object>)yaml.load(url.openStream());
                            mergeConfiguration(config, yamlConfig);
                            break;
                        case "properties":
                            Properties propConfig = new Properties();
                            propConfig.load(url.openStream());
                            mergeConfiguration(config, propConfig);
                            break;

                    }
                    // merge the system properties to the configuration
                    mergeConfiguration(config, filterProperties(System.getProperties(), new String[] {
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_COMMANDER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_SERVERLET),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_PUBLISHER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_SUBSCRIBER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_RPC_AMQP_MASTER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_RPC_AMQP_WORKER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_CFG_AMQP_MASTER),
                        OpflowStringUtil.join(".", OpflowConstant.FRAMEWORK_ID, OpflowConstant.COMP_CFG_AMQP_WORKER),
                    }));
                    // merge the environment variables to the configuration
                    mergeEnvironmentVariables(config);
                } else {
                    configFile = (configFile != null) ? configFile : DEFAULT_CONFIGURATION_FILE;
                    throw new FileNotFoundException("configuration file '" + configFile + "' not found");
                }
            }
            if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                    .put("YAML", OpflowJsonTool.toString(config))
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
        String cfgFromSystem = ENVTOOL.getSystemProperty(DEFAULT_CONFIGURATION_KEY, null);
        if (cfgFromSystem == null) {
            cfgFromSystem = configFile;
        }
        if (cfgFromSystem == null) {
            cfgFromSystem = ENVTOOL.getEnvironVariable(DEFAULT_CONFIGURATION_ENV, null);
        }
        if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                .put("configFile", cfgFromSystem)
                .text("detected configuration file")
                .stringify());
        if (cfgFromSystem == null) {
            url = OpflowUtil.getResource(DEFAULT_CONFIGURATION_FILE);
            if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
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
        if (LOG_TRACER.ready(LOG, Level.TRACE)) LOG.trace(LOG_TRACER
                .put("configFile", url)
                .text("final configuration url")
                .stringify());
        return url;
    }
    
    public static String getConfigurationExtension(URL url) {
        String filename = url.getFile();
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
    
    public static Map<String, Object> mergeEnvironmentVariables(Map<String, Object> target) {
        return mergeEnvironmentVariables(null, target);
    }
    
    public static Map<String, Object> mergeEnvironmentVariables(String prefix, Map<String, Object> target) {
        final String[] prefixes;
        if (prefix != null && prefix.length() > 0) {
            prefixes = new String[] { OpflowStringUtil.convertReservedCharsToLodash(prefix) };
        } else {
            prefixes = null;
        }
        return OpflowObjectTree.traverseTree(target, new OpflowObjectTree.LeafUpdater() {
            @Override
            public Object transform(String[] path, Object value) {
                if (prefixes != null) {
                    path = OpflowCollectionUtil.mergeArrays(prefixes, path);
                }
                String varName = OpflowStringUtil.join("_", path).toUpperCase();
                String envStr = OpflowEnvTool.instance.getEnvironVariable(varName, null);
                if (envStr != null) {
                    return envStr;
                }
                return value;
            }
        });
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
    
    public static Map<String, Object> mergeConfiguration(Map<String, Object> target, Map<String, Object> source) {
        return OpflowObjectTree.merge(target, source);
    }
    
    public static Properties filterProperties(Properties source, String filter) {
        return filterProperties(source, new String[] { filter });
    }
    
    public static Properties filterProperties(Properties source, String[] filters) {
        Properties result = new Properties();
        if (source == null) {
            return null;
        }
        if (filters == null || filters.length == 0) {
            return new Properties(source);
        }
        for(Map.Entry<Object, Object> entry: source.entrySet()) {
            String key = entry.getKey().toString();
            for (String filter : filters) {
                if (key.startsWith(filter)) {
                    result.put(key, source.get(key));
                    break;
                }
            }
        }
        return result;
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
    
    private static String getPropertiesAsString(Properties prop) {
        StringWriter writer = new StringWriter();
        prop.list(new PrintWriter(writer));
        return writer.getBuffer().toString();
    }
}
