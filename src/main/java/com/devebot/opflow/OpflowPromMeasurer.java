package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowObjectTree;
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
        private long directRetain = 0;
        private long directRescue = 0;
        private long remote = 0;
        private long remoteSuccess = 0;
        private long remoteFailure = 0;
        private long remoteTimeout = 0;

        public RpcInvocationCounter() {
        }

        public synchronized void incDirectRescue() {
            this.total++;
            this.direct++;
            this.directRescue++;
        }
        
        public synchronized void incDirectRetain() {
            this.total++;
            this.direct++;
            this.directRetain++;
        }

        public synchronized void incRemoteSuccess() {
            this.total++;
            this.remote++;
            this.remoteSuccess++;
        }

        public synchronized void incRemoteFailure() {
            this.total++;
            this.remote++;
            this.remoteFailure++;
        }

        public synchronized void incRemoteTimeout() {
            this.total++;
            this.remote++;
            this.remoteTimeout++;
        }

        private synchronized RpcInvocationCounter copy() {
            RpcInvocationCounter that = new RpcInvocationCounter();
            that.startTime = this.startTime;
            that.total = this.total;
            that.direct = this.direct;
            that.directRescue = this.directRescue;
            that.directRetain = this.directRetain;
            that.remote = this.remote;
            that.remoteSuccess = this.remoteSuccess;
            that.remoteFailure = this.remoteFailure;
            that.remoteTimeout = this.remoteTimeout;
            return that;
        }

        public synchronized void reset() {
            this.startTime = new Date();
            this.total = 0;
            this.direct = 0;
            this.directRescue = 0;
            this.directRetain = 0;
            this.remote = 0;
            this.remoteSuccess = 0;
            this.remoteFailure = 0;
            this.remoteTimeout = 0;
        }

        public Map<String, Object> toMap() {
            return toMap(true);
        }

        private double calcMessageRate(long count, long ms) {
            if (ms == 0) {
                return -1.0;
            }
            double rate = (1000.0 * count) / ms;
            return Math.round(rate * 100D) / 100D;
        }
        
        private String formatMessageRate(double rate) {
            return String.format("%.2f req/s", rate);
        }
        
        public Map<String, Object> toMap(boolean cloned) {
            RpcInvocationCounter that = cloned ? this.copy() : this;
            Date currentTime = new Date();
            long elapsedTime = (currentTime.getTime() - that.startTime.getTime());
            double directRate = calcMessageRate(that.direct, elapsedTime);
            double remoteRate = calcMessageRate(that.remote, elapsedTime);
            return OpflowObjectTree.buildMap()
                    .put("rpcInvocationTotal", that.total)
                    .put("rpcOverDirectWorker", OpflowObjectTree.buildMap()
                            .put("rate", formatMessageRate(directRate))
                            .put("rateNumber", directRate)
                            .put("total", that.direct)
                            .put("rescue", that.directRescue)
                            .put("retain", that.directRetain)
                            .toMap())
                    .put("rpcOverRemoteWorker", OpflowObjectTree.buildMap()
                            .put("rate", formatMessageRate(remoteRate))
                            .put("rateNumber", remoteRate)
                            .put("total", that.remote)
                            .put("ok", that.remoteSuccess)
                            .put("failed", that.remoteFailure)
                            .put("timeout", that.remoteTimeout)
                            .toMap())
                    .put("startTime", OpflowDateTime.toISO8601UTC(that.startTime))
                    .put("elapsedTime", OpflowDateTime.printElapsedTime(that.startTime, currentTime))
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
                            switch (status) {
                                case "rescue":
                                    counter.incDirectRescue();
                                    break;
                                case "retain":
                                    counter.incDirectRetain();
                                    break;
                            }
                            break;
                        case "detached_worker":
                            switch (status) {
                                case "ok":
                                    counter.incRemoteSuccess();
                                    break;
                                case "failed":
                                    counter.incRemoteFailure();
                                    break;
                                case "timeout":
                                    counter.incRemoteTimeout();
                                    break;
                            }
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
