package com.devebot.opflow.services;

import com.devebot.opflow.OpflowRpcInvocationHandler;
import static com.devebot.opflow.OpflowCommander.KEEP_LOGIC_CLEARLY;
import com.devebot.opflow.OpflowConstant;
import com.devebot.opflow.OpflowConnector;
import com.devebot.opflow.OpflowDiscoveryMaster;
import com.devebot.opflow.OpflowEngine;
import com.devebot.opflow.OpflowInfoCollector;
import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowPromMeasurer;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowRpcAmqpMaster;
import com.devebot.opflow.OpflowRpcHttpMaster;
import com.devebot.opflow.OpflowRpcInvocationCounter;
import com.devebot.opflow.OpflowRpcObserver;
import com.devebot.opflow.OpflowRpcWatcher;
import com.devebot.opflow.OpflowThroughput;
import com.devebot.opflow.exception.OpflowConnectorNotFoundException;
import com.devebot.opflow.exception.OpflowDiscoveryConnectionException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowSystemInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author cuoi
 */
public class OpflowInfoCollectorMaster implements OpflowInfoCollector {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    private final String componentId;
    private final OpflowPromMeasurer measurer;
    private final OpflowRestrictorMaster restrictor;
    private final Map<String, OpflowConnector> connectors;
    private final OpflowThroughput.Meter speedMeter;
    private final OpflowDiscoveryMaster discoveryMaster;
    private final OpflowRpcObserver rpcObserver;
    private final OpflowRpcWatcher rpcWatcher;
    private final String serviceName;
    private final Date startTime;

    public OpflowInfoCollectorMaster(String componentId,
            OpflowPromMeasurer measurer,
            OpflowRestrictorMaster restrictor,
            Map<String, OpflowConnector> connectors,
            OpflowThroughput.Meter speedMeter,
            OpflowDiscoveryMaster discoveryMaster,
            OpflowRpcObserver rpcObserver,
            OpflowRpcWatcher rpcWatcher,
            String serviceName
    ) {
        this.componentId = componentId;
        this.measurer = measurer;
        this.restrictor = restrictor;
        this.connectors = connectors;
        this.speedMeter = speedMeter;
        this.discoveryMaster = discoveryMaster;
        this.rpcObserver = rpcObserver;
        this.rpcWatcher = rpcWatcher;
        this.serviceName = serviceName;
        this.startTime = new Date();
    }

    @Override
    public Map<String, Object> collect() {
        return collect(new HashMap<>());
    }

    @Override
    public Map<String, Object> collect(String scope) {
        return collect(OpflowObjectTree.<Boolean>buildMap()
                .put((scope == null) ? SCOPE_PING : scope, true)
                .toMap());
    }

    private boolean checkOption(Map<String, Boolean> options, String optionName) {
        Boolean opt = options.get(optionName);
        return opt != null && opt;
    }

    @Override
    public Map<String, Object> collect(Map<String, Boolean> options) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String connectorName : connectors.keySet()) {
            info.put(connectorName, collect(connectorName, options));
        }
        return info;
    }
    
    public Map<String, Object> collect(String connectorName, Map<String, Boolean> options) {
        final OpflowConnector connector = connectors.get(connectorName);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + connectorName + "] not found");
        }
        
        final Map<String, Boolean> flag = (options != null) ? options : new HashMap<String, Boolean>();

        OpflowObjectTree.Builder root = OpflowObjectTree.buildMap();

        root.put(OpflowConstant.INSTANCE_ID, OpflowLogTracer.getInstanceId());

        root.put(OpflowConstant.COMP_COMMANDER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.COMPONENT_ID, componentId);

                // DiscoveryClient information
                if (checkOption(flag, OpflowInfoCollector.SCOPE_INFO)) {
                    if (discoveryMaster != null) {
                        opts.put(OpflowConstant.COMP_DISCOVERY_CLIENT, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                opt2.put(OpflowConstant.COMPONENT_ID, discoveryMaster.getComponentId());
                                opt2.put("serviceName", serviceName);
                                if (serviceName != null) {
                                    try {
                                        opt2.put("connection", "ok");
                                        opt2.put("services", discoveryMaster.getService(serviceName));
                                    }
                                    catch (OpflowDiscoveryConnectionException e) {
                                        opt2.put("connection", "failed");
                                    }
                                }
                            }
                        }).toMap());
                    } else {
                        opts.put(OpflowConstant.COMP_DISCOVERY_CLIENT, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_ENABLED, false)
                                .toMap());
                    }
                }

                // Publisher information
                final OpflowPubsubHandler publisher = connector.getPublisher();
                if (checkOption(flag, SCOPE_INFO)) {
                    if (publisher != null) {
                        opts.put(OpflowConstant.COMP_PUBLISHER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                OpflowEngine engine = publisher.getEngine();
                                opt2.put(OpflowConstant.COMPONENT_ID, publisher.getComponentId());
                                opt2.put(OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_NAME, engine.getExchangeName());
                                opt2.put(OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_TYPE, engine.getExchangeType());
                                opt2.put(OpflowConstant.OPFLOW_PUBSUB_EXCHANGE_DURABLE, engine.getExchangeDurable());
                                opt2.put(OpflowConstant.OPFLOW_PUBSUB_ROUTING_KEY, engine.getRoutingKey());
                            }
                        }).toMap());
                    } else {
                        opts.put(OpflowConstant.COMP_PUBLISHER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_ENABLED, false)
                                .toMap());
                    }
                }

                // RPC AMQP Master information
                final OpflowRpcAmqpMaster amqpMaster = connector.getAmqpMaster();
                if (amqpMaster != null) {
                    opts.put(OpflowConstant.COMP_RPC_AMQP_MASTER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opt2) {
                            OpflowEngine engine = amqpMaster.getEngine();

                            opt2.put(OpflowConstant.COMPONENT_ID, amqpMaster.getComponentId());
                            opt2.put(OpflowConstant.OPFLOW_COMMON_APP_ID, engine.getApplicationId());

                            opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, engine.getExchangeName());
                            if (checkOption(flag, SCOPE_INFO)) {
                                opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_TYPE, engine.getExchangeType());
                                opt2.put(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_DURABLE, engine.getExchangeDurable());
                            }
                            opt2.put(OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY, engine.getRoutingKey());

                            opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME, amqpMaster.getResponseQueueName());
                            if (checkOption(flag, SCOPE_INFO)) {
                                opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_DURABLE, amqpMaster.getResponseQueueDurable());
                                opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_EXCLUSIVE, amqpMaster.getResponseQueueExclusive());
                                opt2.put(OpflowConstant.OPFLOW_RESPONSE_QUEUE_AUTO_DELETE, amqpMaster.getResponseQueueAutoDelete());
                            }

                            opt2.put(OpflowConstant.OPFLOW_COMMON_CHANNEL, OpflowObjectTree.buildMap()
                                    .put(OpflowConstant.OPFLOW_COMMON_PROTO_VERSION, CONST.OPFLOW_PROTOCOL_VERSION)
                                    .put(OpflowConstant.AMQP_PARAM_MESSAGE_TTL, amqpMaster.getExpiration())
                                    .put("headers", CONST.getAMQPHeaderInfo(), checkOption(flag, SCOPE_INFO))
                                    .toMap());
                        }
                    }).toMap());
                }

                // RPC HTTP Master information
                final OpflowRpcHttpMaster httpMaster = connector.getHttpMaster();
                if (httpMaster != null) {
                    opts.put(OpflowConstant.COMP_RPC_HTTP_MASTER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opt2) {
                            opt2.put(OpflowConstant.COMPONENT_ID, httpMaster.getComponentId());
                            opt2.put(OpflowConstant.OPFLOW_COMMON_CHANNEL, OpflowObjectTree.buildMap()
                                    .put(OpflowConstant.OPFLOW_COMMON_PROTO_VERSION, CONST.OPFLOW_PROTOCOL_VERSION)
                                    .put(OpflowConstant.HTTP_MASTER_PARAM_CALL_TIMEOUT, httpMaster.getCallTimeout())
                                    .put(OpflowConstant.HTTP_MASTER_PARAM_PUSH_TIMEOUT, httpMaster.getWriteTimeout())
                                    .put(OpflowConstant.HTTP_MASTER_PARAM_PULL_TIMEOUT, httpMaster.getReadTimeout())
                                    .put("headers", CONST.getHTTPHeaderInfo(), checkOption(flag, SCOPE_INFO))
                                    .toMap());
                        }
                    }).toMap());
                }

                // RPC mappings
                Map<String, OpflowRpcInvocationHandler> handlers = connector.getMappings();
                if (checkOption(flag, SCOPE_INFO)) {
                    opts.put("mappings", renderRpcInvocationHandlers(handlers));
                }

                // restrictor information
                if (checkOption(flag, SCOPE_INFO)) {
                    if (restrictor != null) {
                        opts.put(OpflowConstant.COMP_RESTRICTOR, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                            @Override
                            public void transform(Map<String, Object> opt2) {
                                int availablePermits = restrictor.getSemaphorePermits();
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_ENABLED, restrictor.isPauseEnabled());
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_TIMEOUT, restrictor.getPauseTimeout());
                                if (restrictor.isPaused()) {
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_STATUS, "on");
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_ELAPSED_TIME, restrictor.getPauseElapsed());
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_DURATION, restrictor.getPauseDuration());
                                } else {
                                    opt2.put(OpflowConstant.OPFLOW_RESTRICT_PAUSE_STATUS, "off");
                                }
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_PERMITS, restrictor.getSemaphoreLimit());
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_USED_PERMITS, restrictor.getSemaphoreLimit() - availablePermits);
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_FREE_PERMITS, availablePermits);
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_ENABLED, restrictor.isSemaphoreEnabled());
                                opt2.put(OpflowConstant.OPFLOW_RESTRICT_SEMAPHORE_TIMEOUT, restrictor.getSemaphoreTimeout());
                            }
                        }).toMap());
                    } else {
                        opts.put(OpflowConstant.COMP_RESTRICTOR, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_ENABLED, false)
                                .toMap());
                    }
                }

                // rpcObserver information
                if (checkOption(flag, SCOPE_INFO)) {
                    if (rpcObserver != null) {
                        opts.put(OpflowConstant.COMP_RPC_OBSERVER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_ENABLED, rpcObserver.isThreadPoolEnabled())
                                .put(OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_TYPE, rpcObserver.getThreadPoolType())
                                .put(OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_SIZE, rpcObserver.getThreadPoolSize(), "fixed".equals(rpcObserver.getThreadPoolType()))
                                .put(OpflowConstant.OPFLOW_COUNSELOR_THREAD_POOL_USED, rpcObserver.isThreadPoolUsed())
                                .put(OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_ENABLED, rpcObserver.isTrimmingEnabled())
                                .put(OpflowConstant.OPFLOW_COUNSELOR_TRIMMING_TIME_DELAY, rpcObserver.getTrimmingTimeDelay())
                                .put(OpflowConstant.OPFLOW_COUNSELOR_KEEP_ALIVE_TIMEOUT, rpcObserver.getKeepAliveTimeout())
                                .toMap());
                    }
                }

                // rpcWatcher information
                if (checkOption(flag, SCOPE_INFO)) {
                    if (rpcWatcher != null) {
                        opts.put(OpflowConstant.COMP_RPC_WATCHER, OpflowObjectTree.buildMap()
                                .put(OpflowConstant.OPFLOW_COMMON_ENABLED, rpcWatcher.isEnabled())
                                .put(OpflowConstant.OPFLOW_COMMON_INTERVAL, rpcWatcher.getInterval())
                                .put(OpflowConstant.OPFLOW_COMMON_COUNT, rpcWatcher.getCount())
                                .toMap());
                    }
                }

                // promExporter information
                if (checkOption(flag, SCOPE_INFO)) {
                    Map<String, Object> info = measurer.getServiceInfo();
                    if (info != null) {
                        opts.put(OpflowConstant.COMP_PROM_EXPORTER, info);
                    }
                }

                // serve-time & uptime
                if (checkOption(flag, SCOPE_INFO)) {
                    Date currentTime = new Date();
                    opts.put(OpflowConstant.INFO_SECTION_RUNTIME, OpflowObjectTree.buildMap()
                        .put(OpflowConstant.OPFLOW_COMMON_PID, OpflowSystemInfo.getPid())
                            .put(OpflowConstant.OPFLOW_COMMON_THREAD_COUNT, Thread.activeCount())
                            .put(OpflowConstant.OPFLOW_COMMON_CPU_USAGE, OpflowSystemInfo.getCpuUsage())
                            .put(OpflowConstant.OPFLOW_COMMON_MEMORY_USAGE, OpflowSystemInfo.getMemUsage().toMap())
                            .put(OpflowConstant.OPFLOW_COMMON_OS_INFO, OpflowSystemInfo.getOsInfo())
                            .put(OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP, startTime)
                            .put(OpflowConstant.OPFLOW_COMMON_CURRENT_TIMESTAMP, currentTime)
                            .put(OpflowConstant.OPFLOW_COMMON_UPTIME, OpflowDateTime.printElapsedTime(startTime, currentTime))
                            .toMap());
                }

                // git commit information
                if (checkOption(flag, SCOPE_INFO)) {
                    opts.put(OpflowConstant.INFO_SECTION_SOURCE_CODE, OpflowObjectTree.buildMap()
                            .put("server", OpflowSystemInfo.getGitInfo("META-INF/scm/service-master/git-info.json"))
                            .put(OpflowConstant.FRAMEWORK_ID, OpflowSystemInfo.getGitInfo())
                            .toMap());
                }
            }
        }).toMap());

        // current serverlets
        if (checkOption(flag, SCOPE_INFO)) {
            if (rpcObserver != null && (connector.getAmqpMaster() != null || connector.getHttpMaster() != null)) {
                Collection<OpflowRpcObserver.Manifest> serverlets = rpcObserver.summary();
                root.put(OpflowConstant.COMP_SERVERLET, OpflowObjectTree.buildMap()
                        .put(OpflowConstant.OPFLOW_COMMON_CONGESTIVE, rpcObserver.isCongestive())
                        .put("total", serverlets.size())
                        .put("details", serverlets)
                        .toMap());
            }
        }

        return root.toMap();
    }

    @Override
    public Map<String, Object> traffic(Map<String, Boolean> options) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String connectorName : connectors.keySet()) {
            info.put(connectorName, traffic(connectorName, options));
        }
        return info;
    }
    
    public Map<String, Object> traffic(String connectorName, Map<String, Boolean> options) {
        final OpflowConnector connector = connectors.get(connectorName);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + connectorName + "] not found");
        }
        
        final Map<String, Boolean> flag = (options != null) ? options : new HashMap<String, Boolean>();

        Map<String, Object> metrics = OpflowObjectTree.buildMap().toMap();

        // update the RPC invocation counters
        OpflowPromMeasurer _measurer = connector.getMeasurer();
        if (_measurer != null) {
            OpflowRpcInvocationCounter counter = _measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER);
            if (counter != null) {
                OpflowObjectTree.merge(metrics, counter.toMap(true, checkOption(flag, SCOPE_MESSAGE_RATE)));
            }
        }

        // update the RPC invocation throughput
        OpflowThroughput.Tuple _speedMeter = connector.getSpeedMeter();
        if (_speedMeter != null && checkOption(flag, SCOPE_THROUGHPUT)) {
            if (checkOption(flag, SCOPE_LATEST_SPEED)) {
                OpflowObjectTree.merge(metrics, _speedMeter.export(1));
            } else {
                OpflowObjectTree.merge(metrics, _speedMeter.export());
            }
        }

        // size of the callback queue
        OpflowRpcAmqpMaster amqpMaster = connector.getAmqpMaster();
        if (amqpMaster != null) {
            if (KEEP_LOGIC_CLEARLY) {
                OpflowObjectTree.merge(metrics, OpflowObjectTree.buildMap()
                        .put(OpflowRpcInvocationCounter.LABEL_RPC_REMOTE_AMQP_WORKER, OpflowObjectTree.buildMap()
                                .put("waitingReqTotal", OpflowObjectTree.buildMap()
                                        .put("current", amqpMaster.getActiveRequestTotal())
                                        .put("top", amqpMaster.getMaxWaitingRequests())
                                        .toMap())
                                .toMap())
                        .toMap());
            } else {
                Map<String, Object> parentOfQueueInfo;
                Object remoteAmqpWorkerInfo = metrics.get(OpflowRpcInvocationCounter.LABEL_RPC_REMOTE_AMQP_WORKER);
                if (remoteAmqpWorkerInfo instanceof Map) {
                    parentOfQueueInfo = (Map<String, Object>) remoteAmqpWorkerInfo;
                } else {
                    parentOfQueueInfo = OpflowObjectTree.buildMap().toMap();
                    metrics.put(OpflowRpcInvocationCounter.LABEL_RPC_REMOTE_AMQP_WORKER, parentOfQueueInfo);
                }
                parentOfQueueInfo.put("waitingReqTotal", OpflowObjectTree.buildMap()
                        .put("current", amqpMaster.getActiveRequestTotal())
                        .put("top", amqpMaster.getMaxWaitingRequests())
                        .toMap());
            }
        }

        return OpflowObjectTree.buildMap()
                .put("metrics", metrics)
                .put("metadata", speedMeter.getMetadata())
                .toMap();
    }

    protected static List<Map<String, Object>> renderRpcInvocationHandlers(Map<String, OpflowRpcInvocationHandler> handlers) {
        List<Map<String, Object>> mappingInfos = new ArrayList<>();
        for(final Map.Entry<String, OpflowRpcInvocationHandler> entry : handlers.entrySet()) {
            final OpflowRpcInvocationHandler val = entry.getValue();
            mappingInfos.add(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put("class", entry.getKey());
                    opts.put("methods", val.getMethodInfos());
                    if (val.getNativeWorkerClassName() != null) {
                        opts.put("nativeWorkerClassName", val.getNativeWorkerClassName());
                    }
                    opts.put("publisherActive", val.isPublisherActive());
                    opts.put("publisherAvailable", val.isPublisherAvailable());
                    opts.put("amqpWorkerActive", val.isRemoteAMQPWorkerActive());
                    opts.put("amqpWorkerAvailable", val.isRemoteAMQPWorkerAvailable());
                    opts.put("httpWorkerActive", val.isRemoteHTTPWorkerActive());
                    opts.put("httpWorkerAvailable", val.isRemoteHTTPWorkerAvailable());
                    opts.put("nativeWorkerActive", val.isNativeWorkerActive());
                    opts.put("nativeWorkerAvailable", val.isNativeWorkerAvailable());
                }
            }).toMap());
        }
        return mappingInfos;
    }
}