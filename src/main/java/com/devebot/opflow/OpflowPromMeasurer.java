package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {

    public static enum GaugeAction { INC, DEC }
    
    public abstract void updateComponentInstance(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void removeComponentInstance(String instanceType, String instanceId);
    
    public abstract void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action);
    
    public abstract void updateActiveChannel(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void countRpcInvocation(String moduleName, String eventName, String routineId, String status);
    
    public abstract RpcInvocationCounter getRpcInvocationCounter(String moduleName);
    
    public abstract Map<String, Object> resetRpcInvocationCounter();
    
    private static PipeMeasurer instance = new PipeMeasurer();

    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return instance;
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (OpflowUtil.isComponentEnabled(kwargs)) {
            if (!instance.hasShadow()) {
                instance.setShadow(new OpflowPromExporter(kwargs));
            }
        }
        return instance;
    }
    
    public static class RpcInvocationCounter {
        private Date startTime = new Date();
        private long total = 0;
        private long direct = 0;
        private long remote = 0;

        public RpcInvocationCounter() {
        }
        
        public synchronized void incDirect() {
            this.total++;
            this.direct++;
        }
        
        public synchronized void incRemote() {
            this.total++;
            this.remote++;
        }
        
        public synchronized RpcInvocationCounter copy() {
            RpcInvocationCounter that = new RpcInvocationCounter();
            that.startTime = this.startTime;
            that.total = this.total;
            that.direct = this.direct;
            that.remote = this.remote;
            return that;
        }
        
        public synchronized void reset() {
            this.startTime = new Date();
            this.total = 0;
            this.direct = 0;
            this.remote = 0;
        }
        
        public synchronized Map<String, Object> toMap() {
            return OpflowUtil.buildOrderedMap()
                    .put("rpcInvocationTotal", this.total)
                    .put("rpcOverDirectWorkerTotal", this.direct)
                    .put("rpcOverRemoteWorkerTotal", this.remote)
                    .put("startTime", OpflowDateTime.toISO8601UTC(this.startTime))
                    .toMap();
        }
    }
    
    static class PipeMeasurer extends OpflowPromMeasurer {

        private OpflowPromMeasurer shadow = null;
        private final RpcInvocationCounter counter = new RpcInvocationCounter();

        public PipeMeasurer() {
        }

        public PipeMeasurer(OpflowPromMeasurer shadow) {
            this.shadow = shadow;
        }

        public boolean hasShadow() {
            return this.shadow != null;
        }
        
        public void setShadow(OpflowPromMeasurer shadow) {
            this.shadow = shadow;
        }

        @Override
        public void updateComponentInstance(String instanceType, String instanceId, GaugeAction action) {
            if (shadow != null) {
                shadow.removeComponentInstance(instanceType, instanceId);
            }
        }

        @Override
        public void removeComponentInstance(String instanceType, String instanceId) {
            if (shadow != null) {
                shadow.removeComponentInstance(instanceType, instanceId);
            }
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(factory, connectionType, action);
            }
        }

        @Override
        public void updateActiveChannel(String instanceType, String instanceId, GaugeAction action) {
            if (shadow != null) {
                shadow.updateActiveChannel(instanceType, instanceId, action);
            }
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineId, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(moduleName, eventName, routineId, status);
            }
            switch (moduleName) {
                case "commander": {
                    switch (eventName) {
                        case "reserved_worker":
                            counter.incDirect();
                            break;
                        case "detached_worker":
                            counter.incRemote();
                            break;
                        default:
                            break;
                    }
                    break;
                }
            }
        }
        
        @Override
        public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            return counter.copy();
        }

        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            counter.reset();
            return counter.toMap();
        }
    }
    
    static class NullMeasurer extends OpflowPromMeasurer {

        @Override
        public void updateComponentInstance(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void removeComponentInstance(String instanceType, String instanceId) {
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
        }

        @Override
        public void updateActiveChannel(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineId, String status) {
        }
        
        @Override
        public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            return null;
        }

        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            return null;
        }
    }
    
    public static final OpflowPromMeasurer NULL = new NullMeasurer();
}
