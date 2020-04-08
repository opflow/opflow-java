package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowConnectorNotFoundException;
import com.devebot.opflow.services.OpflowInfoCollectorMaster;
import com.devebot.opflow.services.OpflowRestrictorMaster;
import com.devebot.opflow.services.OpflowTaskSubmitterMaster;
import com.devebot.opflow.supports.OpflowCollectionUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowCommander implements AutoCloseable {
    public final static List<String> SERVICE_BEAN_NAMES = Arrays.asList(new String[] {
        OpflowConstant.COMP_PUBLISHER,
        OpflowConstant.COMP_RPC_AMQP_MASTER,
    });
    
    public final static List<String> SUPPORT_BEAN_NAMES = Arrays.asList(new String[] {
        OpflowConstant.COMP_DISCOVERY_CLIENT,
        OpflowConstant.COMP_REQ_EXTRACTOR,
        OpflowConstant.COMP_RESTRICTOR,
        OpflowConstant.COMP_RPC_HTTP_MASTER,
        OpflowConstant.COMP_RPC_WATCHER,
        OpflowConstant.COMP_GARBAGE_COLLECTOR,
        OpflowConstant.COMP_SPEED_METER,
        OpflowConstant.COMP_PROM_EXPORTER,
        OpflowConstant.COMP_REST_SERVER,
    });
    
    public final static List<String> ALL_BEAN_NAMES = OpflowCollectionUtil.<String>mergeLists(SERVICE_BEAN_NAMES, SUPPORT_BEAN_NAMES);
    
    public final static boolean KEEP_LOGIC_CLEARLY = false;
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowCommander.class);
    
    private final Object runningLock = new Object();
    private volatile boolean runningActive = false;
    
    private final String serviceName;
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowConfig.Loader configLoader;
    private final OpflowDiscoveryMaster discoveryMaster;
    private final OpflowThroughput.Meter speedMeter;
    private OpflowRestrictorMaster restrictor;

    private final Map<String, OpflowConnector> connectors;
    
    private OpflowRpcWatcher rpcWatcher;
    private OpflowRpcObserver rpcObserver;
    private OpflowGarbageCollector garbageCollector;
    private OpflowRestServer restServer;
    private OpflowReqExtractor reqExtractor;

    public OpflowCommander() throws OpflowBootstrapException {
        this(null, null);
    }
    
    public OpflowCommander(OpflowConfig.Loader loader) throws OpflowBootstrapException {
        this(loader, null);
    }

    public OpflowCommander(Map<String, Object> kwargs) throws OpflowBootstrapException {
        this(null, kwargs);
    }

    private OpflowCommander(OpflowConfig.Loader loader, Map<String, Object> kwargs) throws OpflowBootstrapException {
        if (loader != null) {
            configLoader = loader;
        } else {
            configLoader = null;
        }
        if (configLoader != null) {
            kwargs = configLoader.loadConfiguration();
        }
        
        kwargs = OpflowObjectTree.ensureNonNull(kwargs);
        
        serviceName = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_SERVICE_NAME);
        componentId = OpflowUtil.getStringField(kwargs, OpflowConstant.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("commanderId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Commander[${commanderId}][${instanceId}].new()")
                .stringify());
        
        measurer = OpflowPromMeasurer.getInstance(OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PROM_EXPORTER));
        
        Map<String, Object> reqExtractorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REQ_EXTRACTOR);
        if (reqExtractorCfg == null || OpflowUtil.isComponentEnabled(reqExtractorCfg)) {
            reqExtractor = new OpflowReqExtractor(reqExtractorCfg);
        }
        
        Map<String, Object> restrictorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RESTRICTOR);
        if (restrictorCfg == null || OpflowUtil.isComponentEnabled(restrictorCfg)) {
            restrictor = new OpflowRestrictorMaster(OpflowObjectTree.buildMap(restrictorCfg)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .put(OpflowConstant.COMP_MEASURER, measurer)
                    .toMap());
        }
        
        Map<String, Object> defaultConnectorCfg = extractDefaultConnectorCfg(kwargs);
        Map<String, Map<String, Object>> connectorCfgMap = extractConnectorCfgMap(kwargs);
        
        Map<String, Object> rpcObserverCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_OBSERVER);
        rpcObserver = new OpflowRpcObserver(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.COMPONENT_ID, componentId);
            }
        }, rpcObserverCfg).toMap());
        
        if (restrictor != null) {
            restrictor.block();
        }
        
        try {
            Map<String, Object> discoveryClientCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_DISCOVERY_CLIENT);
            if (OpflowUtil.isComponentExplicitEnabled(discoveryClientCfg)) {
                discoveryMaster = new OpflowDiscoveryMaster(componentId, serviceName, discoveryClientCfg);
            } else {
                discoveryMaster = null;
            }
            
            if (discoveryMaster != null && rpcObserver != null) {
                discoveryMaster.subscribe(rpcObserver.getServiceUpdater());
            }
            
            Map<String, Object> speedMeterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_SPEED_METER);
            if (speedMeterCfg == null || OpflowUtil.isComponentEnabled(speedMeterCfg)) {
                speedMeter = (new OpflowThroughput.Meter(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            opts.put(OpflowConstant.COMPONENT_ID, componentId);
                        }
                    }, speedMeterCfg).toMap()));
            } else {
                speedMeter = null;
            }
            
            Map<String, Object> garbageCollectorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_GARBAGE_COLLECTOR);
            if (OpflowUtil.isComponentEnabled(garbageCollectorCfg)) {
                garbageCollector = new OpflowGarbageCollector(OpflowObjectTree.buildMap(garbageCollectorCfg)
                        .put(OpflowConstant.COMPONENT_ID, componentId)
                        .toMap());
            }
            
            // initialize the connectors list
            connectors = OpflowObjectTree.<OpflowConnector>buildMap().toMap();
            
            // instantiate the default connector
            connectors.put(OpflowConnector.DEFAULT_CONNECTOR_NAME, createConnector(
                OpflowConnector.DEFAULT_CONNECTOR_NAME,
                defaultConnectorCfg,
                componentId,
                measurer,
                speedMeter,
                reqExtractor,
                restrictor,
                rpcObserver
            ));
            
            // instantiate the connectors
            for (String connectorName : connectorCfgMap.keySet()) {
                connectors.put(connectorName, createConnector(
                    connectorName,
                    connectorCfgMap.get(connectorName),
                    componentId,
                    measurer,
                    speedMeter,
                    reqExtractor,
                    restrictor,
                    rpcObserver
                ));
            }
            
            Map<String, Object> rpcWatcherCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_WATCHER);
            rpcWatcher = new OpflowRpcWatcher(connectors, garbageCollector, OpflowObjectTree.buildMap(rpcWatcherCfg)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .toMap());
            
            if (rpcObserver != null) {
                rpcObserver.setKeepAliveTimeout(rpcWatcher.getInterval());
            }
            
            Map<String, Object> restServerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REST_SERVER);
            if (OpflowUtil.isComponentImplicitEnabled(restServerCfg)) {
                OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(componentId, measurer, restrictor, connectors, speedMeter, discoveryMaster,
                        rpcObserver, rpcWatcher, reqExtractor, serviceName);

                OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(componentId, measurer, restrictor, connectors, speedMeter, discoveryMaster);

                restServer = new OpflowRestServer(connectors, infoCollector, taskSubmitter, OpflowObjectTree.buildMap(restServerCfg)
                        .put(OpflowConstant.COMPONENT_ID, componentId)
                        .toMap());
            }
        } catch(OpflowBootstrapException exception) {
            this.close();
            throw exception;
        }
        
        measurer.updateComponentInstance(OpflowConstant.COMP_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.INC);
        
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                close();
            }
        });
        
        if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
            .put("measurer", measurer)
            .put("restrictor", restrictor)
            .put("reqExtractor", reqExtractor)
            .put("discoveryMaster", discoveryMaster)
            .put("speedMeter", speedMeter)
            .put("garbageCollector", garbageCollector)
            .put("rpcWatcher", rpcWatcher)
            .put("rpcObserver", rpcObserver)
            .put("restServer", restServer)
            .tags(OpflowCommander.class.getCanonicalName(), "constructor")
            .text("Commander[${commanderId}][${instanceId}].new() for unit testing")
            .stringify());
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
            .text("Commander[${commanderId}][${instanceId}].new() end!")
            .stringify());
    }
    
    public final void serve() {
        synchronized (runningLock) {
            if (!runningActive) {
                runningActive = true;
            } else {
                return;
            }

            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Commander[${commanderId}].serve() begin")
                    .stringify());

            OpflowUUID.serve();

            if (discoveryMaster != null) {
                discoveryMaster.serve();
            }
            if (restServer != null) {
                restServer.serve();
            }
            if (restrictor != null) {
                restrictor.unblock();
            }
            if (rpcWatcher != null) {
                rpcWatcher.serve(); // Timer should be run after the unblock() call
            }
            if (speedMeter != null) {
                speedMeter.serve(); // Timer should be run after the unblock() call
            }
            
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Commander[${commanderId}].serve() end")
                    .stringify());
        }
    }
    
    @Override
    public final void close() {
        synchronized (runningLock) {
            if (runningActive) {
                runningActive = false;
            } else {
                return;
            }

            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Commander[${commanderId}][${instanceId}].close()")
                    .stringify());

            if (restrictor != null) {
                restrictor.block();
            }

            if (speedMeter != null) {
                speedMeter.close();
            }

            if (restServer != null) restServer.close();
            if (rpcWatcher != null) rpcWatcher.close();

            OpflowConnector.applyConnectors(connectors, (OpflowConnector connector) -> {
                boolean ok = true;
                ok = ok && muteCloseException(connector.getPublisher());
                ok = ok && muteCloseException(connector.getAmqpMaster());
                ok = ok && muteCloseException(connector.getHttpMaster());
                return ok;
            });
            
            if (discoveryMaster != null) {
                discoveryMaster.close();
            }
            
            if (rpcObserver != null) rpcObserver.close();

            if (restrictor != null) {
                restrictor.close();
            }

            OpflowUUID.release();

            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("Commander[${commanderId}][${instanceId}].close() end!")
                    .stringify());
        }
    }
    
    public boolean isConnectorAvailable(String connectorName) {
        return connectors.get(connectorName) != null;
    }
    
    @Deprecated
    public void ping(String query) throws Throwable {
        pingWithDefault(query);
    }
    
    public void pingWithDefault(String query) throws Throwable {
        ping(OpflowConnector.DEFAULT_CONNECTOR_NAME, query);
    }
    
    public void ping(String connectorName, String query) throws Throwable {
        getConnectorByName(connectorName).getRpcChecker().send(new OpflowRpcChecker.Ping(query));
    }
    
    @Deprecated
    public <T> T registerType(Class<T> type) {
        return registerTypeWithDefault(type);
    }
    
    @Deprecated
    public <T> T registerType(Class<T> type, T bean) {
        return registerTypeWithDefault(type, bean);
    }
    
    public <T> T registerTypeWithDefault(Class<T> type) {
        return registerType(OpflowConnector.DEFAULT_CONNECTOR_NAME, type, null);
    }
    
    public <T> T registerTypeWithDefault(Class<T> type, T bean) {
        return registerType(OpflowConnector.DEFAULT_CONNECTOR_NAME, type, bean);
    }
    
    public <T> void unregisterTypeWithDefault(Class<T> type) {
        unregisterType(OpflowConnector.DEFAULT_CONNECTOR_NAME, type);
    }
    
    public <T> T registerType(String connectorName, Class<T> type) {
        return registerType(connectorName, type, null);
    }
    
    public <T> T registerType(String connectorName, Class<T> type, T bean) {
        return getConnectorByName(connectorName).registerType(type, bean);
    }
    
    public <T> void unregisterType(String connectorName, Class<T> type) {
        getConnectorByName(connectorName).unregisterType(type);
    }
    
    private OpflowConnector getConnectorByName(String connectorName) {
        OpflowConnector connector = connectors.get(connectorName);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + connectorName + "] not found");
        }
        return connector;
    }
    
    private static boolean hasAnyRemoteMaster(Map<String, Object> connectorCfg) {
        Map<String, Object> amqpMasterCfg = OpflowUtil.getChildMap(connectorCfg, OpflowConstant.COMP_RPC_AMQP_MASTER);
        Map<String, Object> httpMasterCfg = OpflowUtil.getChildMap(connectorCfg, OpflowConstant.COMP_RPC_HTTP_MASTER);
        
        if (OpflowUtil.isComponentEnabled(amqpMasterCfg)) {
            return true;
        }
        
        if (OpflowUtil.isComponentEnabled(httpMasterCfg)) {
            return true;
        }
        
        return false;
    }
    
    private static boolean hasAnyRemoteMaster(Set<Map<String, Object>> connectorCfgMap) {
        if (connectorCfgMap != null) {
            for (Map<String, Object> connectorCfg : connectorCfgMap) {
                if (hasAnyRemoteMaster(connectorCfg)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    private static Map<String, Object> extractDefaultConnectorCfg(Map<String, Object> kwargs) {
        Map<String, Object> target = OpflowObjectTree.buildMap().toMap();
        OpflowUtil.copyParameters(target, kwargs, new String[] {
            OpflowConstant.OPFLOW_COMMON_STRICT,
            OpflowConstant.PARAM_NATIVE_WORKER_ENABLED,
            OpflowConstant.COMP_PUBLISHER,
            OpflowConstant.COMP_RPC_AMQP_MASTER,
            OpflowConstant.COMP_RPC_HTTP_MASTER,
        });
        return target;
    }
    
    private static Map<String, Map<String, Object>> extractConnectorCfgMap(Map<String, Object> kwargs) throws OpflowBootstrapException {
        Map<String, Map<String, Object>> output = OpflowObjectTree.<Map<String, Object>>buildMap().toMap();
        Map<String, Object> connectorCfgMap = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_CONNECTOR);
        if (connectorCfgMap != null) {
            for (String connectorName : connectorCfgMap.keySet()) {
                Map<String, Object> connectorCfg = OpflowUtil.getChildMap(connectorCfgMap, connectorName);
                if (connectorCfg == null) {
                    throw new OpflowBootstrapException("Invalid configuration for the connector[" + connectorName + "]");
                }
                output.put(connectorName, connectorCfg);
            }
        }
        return output;
    }
    
    private static OpflowConnector createConnector(
        String connectorName, Map<String,
        Object> connectorCfg,
        String componentId,
        OpflowPromMeasurer measurer,
        OpflowThroughput.Meter speedMeter,
        OpflowReqExtractor reqExtractor,
        OpflowRestrictorMaster restrictor,
        OpflowRpcObserver rpcObserver
    ) throws OpflowBootstrapException {
        OpflowThroughput.Tuple connectorMeter = speedMeter.register(connectorName);
        OpflowPromMeasurer connectorMeasurer = new OpflowPromMeasurer.PipeMeasurer(measurer);
        
        OpflowConnector connector = new OpflowConnector(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.COMPONENT_ID, componentId);
                opts.put(OpflowConstant.COMP_REQ_EXTRACTOR, reqExtractor);
                opts.put(OpflowConstant.COMP_RESTRICTOR, restrictor);
                opts.put(OpflowConstant.COMP_RPC_OBSERVER, rpcObserver);
                opts.put(OpflowConstant.COMP_MEASURER, connectorMeasurer);
                opts.put(OpflowConstant.COMP_SPEED_METER, connectorMeter);
            }
        }, connectorCfg).toMap());
        
        return connector;
    }
    
    private boolean muteCloseException(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                return false;
            }
        }
        return true;
    }
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
