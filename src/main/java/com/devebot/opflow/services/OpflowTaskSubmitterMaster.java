package com.devebot.opflow.services;

import com.devebot.opflow.OpflowRpcInvocationHandler;
import com.devebot.opflow.OpflowConstant;
import com.devebot.opflow.OpflowConnector;
import com.devebot.opflow.OpflowDiscoveryMaster;
import com.devebot.opflow.OpflowLogTracer;
import com.devebot.opflow.OpflowPromMeasurer;
import com.devebot.opflow.OpflowPubsubHandler;
import com.devebot.opflow.OpflowRpcAmqpMaster;
import com.devebot.opflow.OpflowRpcHttpMaster;
import com.devebot.opflow.OpflowTaskSubmitter;
import com.devebot.opflow.OpflowThroughput;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowConnectorNotFoundException;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cuoi
 */
public class OpflowTaskSubmitterMaster implements OpflowTaskSubmitter {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowTaskSubmitterMaster.class);
    
    private final String componentId;
    private final OpflowPromMeasurer measurer;
    private final OpflowLogTracer logTracer;
    private final OpflowRestrictorMaster restrictor;
    private final Map<String, OpflowConnector> connectors;
    private final OpflowThroughput.Meter speedMeter;
    private final OpflowDiscoveryMaster discoveryMaster;

    public OpflowTaskSubmitterMaster(String componentId,
            OpflowPromMeasurer measurer,
            OpflowRestrictorMaster restrictor,
            Map<String, OpflowConnector> connectors,
            OpflowThroughput.Meter speedMeter,
            OpflowDiscoveryMaster discoveryMaster
    ) {
        this.componentId = componentId;
        this.measurer = measurer;
        this.restrictor = restrictor;
        this.connectors = connectors;
        this.speedMeter = speedMeter;
        this.discoveryMaster = discoveryMaster;
        this.logTracer = OpflowLogTracer.ROOT.branch("taskSubmitterId", componentId);
    }

    @Override
    public Map<String, Object> pause(long duration) {
        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("OpflowTaskSubmitter[${taskSubmitterId}].pause(true) is invoked")
                .stringify());
        if (restrictor == null) {
            return OpflowObjectTree.buildMap()
                    .toMap();
        }
        return restrictor.pause(duration);
    }

    @Override
    public Map<String, Object> unpause() {
        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("OpflowTaskSubmitter[${taskSubmitterId}].unpause() is invoked")
                .stringify());
        if (restrictor == null) {
            return OpflowObjectTree.buildMap()
                    .toMap();
        }
        return restrictor.unpause();
    }

    @Override
    public Map<String, Object> reset() {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String connectorName : connectors.keySet()) {
            info.put(connectorName, reset(connectorName));
        }
        return info;
    }
    
    public Map<String, Object> reset(String connectorName) {
        final OpflowConnector connector = connectors.get(connectorName);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + connectorName + "] not found");
        }
        OpflowRpcAmqpMaster amqpMaster = connector.getAmqpMaster();
        if (amqpMaster == null) {
            return OpflowObjectTree.buildMap()
                    .toMap();
        }
        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("OpflowTaskSubmitter[${taskSubmitterId}].reset() is invoked")
                .stringify());
        amqpMaster.reset();
        return OpflowObjectTree.buildMap()
                .toMap();
    }

    @Override
    public Map<String, Object> resetRpcInvocationCounter() {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String connectorName : connectors.keySet()) {
            info.put(connectorName, resetRpcInvocationCounter(connectorName));
        }
        return info;
    }
    
    public Map<String, Object> resetRpcInvocationCounter(String connectorName) {
        final OpflowConnector connector = connectors.get(connectorName);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + connectorName + "] not found");
        }
        OpflowPromMeasurer _measurer = connector.getMeasurer();
        if (_measurer != null) {
            _measurer.resetRpcInvocationCounter();
        }
        OpflowThroughput.Tuple _speedMeter = connector.getSpeedMeter();
        if (_speedMeter != null) {
            _speedMeter.reset();
        }
        final OpflowRpcAmqpMaster amqpMaster = connector.getAmqpMaster();
        if (amqpMaster != null) {
            amqpMaster.resetCallbackQueueCounter();
        }
        return OpflowObjectTree.buildMap().put("acknowledged", true).toMap();
    }

    @Override
    public Map<String, Object> resetDiscoveryClient() {
        if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .text("OpflowTaskSubmitter[${taskSubmitterId}].resetDiscoveryClient() is invoked")
                .stringify());
        if (discoveryMaster != null) {
            try {
                discoveryMaster.reset();
                return OpflowObjectTree.buildMap().put("result", "ok").toMap();
            }
            catch (Exception e) {
                return OpflowObjectTree.buildMap().put("result", "failed").toMap();
            }
        }
        return OpflowObjectTree.buildMap().put("result", "unsupported").toMap();
    }

    @Override
    public Map<String, Object> activatePublisher(boolean state, Map<String, Object> opts) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String name : connectors.keySet()) {
            info.put(name, activatePublisher(name, state, opts));
        }
        return info;
    }
    
    public Map<String, Object> activatePublisher(String name, boolean state, Map<String, Object> opts) {
        return activateWorker(name, OpflowConstant.COMP_PUBLISHER, state, opts);
    }

    @Override
    public Map<String, Object> activateRemoteAMQPWorker(boolean state, Map<String, Object> opts) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String name : connectors.keySet()) {
            info.put(name, activateRemoteAMQPWorker(name, state, opts));
        }
        return info;
    }
    
    public Map<String, Object> activateRemoteAMQPWorker(String name, boolean state, Map<String, Object> opts) {
        return activateWorker(name, OpflowConstant.COMP_REMOTE_AMQP_WORKER, state, opts);
    }

    @Override
    public Map<String, Object> activateRemoteHTTPWorker(boolean state, Map<String, Object> opts) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String name : connectors.keySet()) {
            info.put(name, activateRemoteHTTPWorker(name, state, opts));
        }
        return info;
    }
    
    public Map<String, Object> activateRemoteHTTPWorker(String name, boolean state, Map<String, Object> opts) {
        return activateWorker(name, OpflowConstant.COMP_REMOTE_HTTP_WORKER, state, opts);
    }

    @Override
    public Map<String, Object> activateNativeWorker(boolean state, Map<String, Object> opts) {
        Map<String, Object> info = OpflowObjectTree.buildMap().toMap();
        for (String name : connectors.keySet()) {
            info.put(name, activateNativeWorker(name, state, opts));
        }
        return info;
    }
    
    public Map<String, Object> activateNativeWorker(String name, boolean state, Map<String, Object> opts) {
        return activateWorker(name, OpflowConstant.COMP_NATIVE_WORKER, state, opts);
    }

    private Map<String, Object> activateWorker(String name, String type, boolean state, Map<String, Object> opts) {
        // get the OpflowConnector object
        if (name == null) {
            name = OpflowConnector.DEFAULT_CONNECTOR_NAME;
        }
        final OpflowConnector connector = connectors.get(name);
        if (connector == null) {
            throw new OpflowConnectorNotFoundException("Connector[" + name + "] not found");
        }
        // update the state of the RpcInvocationHandlers
        Map<String, OpflowRpcInvocationHandler> handlers = connector.getMappings();
        String clazz = OpflowUtil.getStringField(opts, "class");
        for(final Map.Entry<String, OpflowRpcInvocationHandler> entry : handlers.entrySet()) {
            final String key = entry.getKey();
            final OpflowRpcInvocationHandler handler = entry.getValue();
            if (clazz != null) {
                if (clazz.equals(key)) {
                    activateWorkerForRpcInvocation(handler, type, state);
                }
            } else {
                activateWorkerForRpcInvocation(handler, type, state);
            }
        }
        return OpflowObjectTree.buildMap()
                .put("mappings", OpflowInfoCollectorMaster.renderRpcInvocationHandlers(handlers))
                .toMap();
    }

    private void activateWorkerForRpcInvocation(OpflowRpcInvocationHandler handler, String type, boolean state) {
        if (OpflowConstant.COMP_PUBLISHER.equals(type)) {
            handler.setPublisherActive(state);
            return;
        }
        if (OpflowConstant.COMP_REMOTE_AMQP_WORKER.equals(type)) {
            handler.setRemoteAMQPWorkerActive(state);
            return;
        }
        if (OpflowConstant.COMP_REMOTE_HTTP_WORKER.equals(type)) {
            handler.setRemoteHTTPWorkerActive(state);
            return;
        }
        if (OpflowConstant.COMP_NATIVE_WORKER.equals(type)) {
            handler.setNativeWorkerActive(state);
            return;
        }
    }
}
