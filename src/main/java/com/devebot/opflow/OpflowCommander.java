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
        
        Map<String, Object> restrictorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RESTRICTOR);
        
        if (restrictorCfg == null || OpflowUtil.isComponentEnabled(restrictorCfg)) {
            restrictor = new OpflowRestrictorMaster(OpflowObjectTree.buildMap(restrictorCfg)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .put(OpflowConstant.COMP_MEASURER, measurer)
                    .toMap());
        }
        
        if (restrictor != null) {
            restrictor.block();
        }
        
        Map<String, Object> discoveryClientCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_DISCOVERY_CLIENT);
        
        if (OpflowUtil.isComponentExplicitEnabled(discoveryClientCfg)) {
            discoveryMaster = new OpflowDiscoveryMaster(componentId, serviceName, discoveryClientCfg);
        } else {
            discoveryMaster = null;
        }
        
        Map<String, Object> reqExtractorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REQ_EXTRACTOR);
        Map<String, Object> rpcObserverCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_OBSERVER);
        Map<String, Object> rpcWatcherCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_WATCHER);
        Map<String, Object> garbageCollectorCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_GARBAGE_COLLECTOR);
        Map<String, Object> restServerCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_REST_SERVER);
        
        try {
            if (reqExtractorCfg == null || OpflowUtil.isComponentEnabled(reqExtractorCfg)) {
                reqExtractor = new OpflowReqExtractor(reqExtractorCfg);
            }
            
            rpcObserver = new OpflowRpcObserver(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(OpflowConstant.COMPONENT_ID, componentId);
                }
            }, rpcObserverCfg).toMap());
            
            if (discoveryMaster != null) {
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
            
            if (OpflowUtil.isComponentEnabled(garbageCollectorCfg)) {
                garbageCollector = new OpflowGarbageCollector(OpflowObjectTree.buildMap(garbageCollectorCfg)
                        .put(OpflowConstant.COMPONENT_ID, componentId)
                        .toMap());
            }

            String connectorName = OpflowConnector.DEFAULT_CONNECTOR_NAME;
            Map<String, Object> connectorCfg = kwargs;
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
            
            connectors = OpflowObjectTree.<OpflowConnector>buildMap()
                    .put(connectorName, connector)
                    .toMap();
            
            rpcWatcher = new OpflowRpcWatcher(connectors, garbageCollector, OpflowObjectTree.buildMap(rpcWatcherCfg)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .toMap());
            
            rpcObserver.setKeepAliveTimeout(rpcWatcher.getInterval());

            OpflowInfoCollector infoCollector = new OpflowInfoCollectorMaster(componentId, measurer, restrictor, connectors, speedMeter,
                    discoveryMaster, rpcObserver, rpcWatcher, serviceName);

            OpflowTaskSubmitter taskSubmitter = new OpflowTaskSubmitterMaster(componentId, measurer, restrictor, connectors, speedMeter,
                    discoveryMaster);

            restServer = new OpflowRestServer(connectors, infoCollector, taskSubmitter, OpflowObjectTree.buildMap(restServerCfg)
                    .put(OpflowConstant.COMPONENT_ID, componentId)
                    .toMap());
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
                rpcWatcher.serve();
            }
            if (speedMeter != null) {
                speedMeter.serve();
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
    
    public void ping(String query) throws Throwable {
        getDefaultConnector().getRpcChecker().send(new OpflowRpcChecker.Ping(query));
    }
    
    public <T> T registerType(Class<T> type) {
        return registerType(type, null);
    }

    public <T> T registerType(Class<T> type, T bean) {
        return getDefaultConnector().registerType(type, bean);
    }
    
    private OpflowConnector getDefaultConnector() {
        OpflowConnector connector = connectors.get(OpflowConnector.DEFAULT_CONNECTOR_NAME);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + OpflowConnector.DEFAULT_CONNECTOR_NAME + "] not found");
        }
        return connector;
    }
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_COMMANDER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
