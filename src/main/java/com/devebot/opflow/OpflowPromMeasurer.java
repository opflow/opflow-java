package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowMathUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    public static final String LABEL_RPC_INVOCATION_TOTAL = "rpcInvocationTotal";
    public static final String LABEL_RPC_DIRECT_WORKER = "rpcOverDirectWorker";
    public static final String LABEL_RPC_REMOTE_WORKER = "rpcOverRemoteWorker";
    
    public static enum GaugeAction { INC, DEC }
    
    public abstract void updateComponentInstance(String componentType, String componentId, GaugeAction action);
    
    public abstract void removeComponentInstance(String componentType, String componentId);
    
    public abstract void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action);
    
    public abstract void updateActiveChannel(String componentType, String componentId, GaugeAction action);
    
    public abstract void countRpcInvocation(String moduleName, String eventName, String routineSignature, String status);
    
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
            return OpflowMathUtil.round(rate, 1);
        }
        
        private String formatMessageRate(double rate) {
            return String.format("%.1f req/s", rate);
        }
        
        public Map<String, Object> toMap(final boolean cloned) {
            return toMap(cloned, false);
        }
        
        public Map<String, Object> toMap(final boolean cloned, final boolean verbose) {
            final RpcInvocationCounter that = cloned ? this.copy() : this;
            final Date currentTime = new Date();
            final long elapsedTime = (currentTime.getTime() - that.startTime.getTime());
            return OpflowObjectTree.buildMap()
                    .put(LABEL_RPC_INVOCATION_TOTAL, that.total)
                    .put(LABEL_RPC_DIRECT_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            opts.put("total", that.direct);
                            opts.put("rescue", that.directRescue);
                            opts.put("retain", that.directRetain);
                            if (verbose) {
                                double directRate = calcMessageRate(that.direct, elapsedTime);
                                opts.put("rate", formatMessageRate(directRate));
                                opts.put("rateNumber", directRate);
                            }
                        }
                    }).toMap())
                    .put(LABEL_RPC_REMOTE_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            opts.put("total", that.remote);
                            opts.put("ok", that.remoteSuccess);
                            opts.put("failed", that.remoteFailure);
                            opts.put("timeout", that.remoteTimeout);
                            if (verbose) {
                                double remoteRate = calcMessageRate(that.remote, elapsedTime);
                                opts.put("rate", formatMessageRate(remoteRate));
                                opts.put("rateNumber", remoteRate);
                            }
                        }
                    }).toMap())
                    .put("startTime", that.startTime)
                    .put("elapsedTime", OpflowDateTime.printElapsedTime(that.startTime, currentTime))
                    .toMap();
        }
        
        public OpflowThroughput.Source getDirectWorkerInfoSource() {
            return new OpflowThroughput.Source() {
                @Override
                public long getValue() {
                    return direct;
                }

                @Override
                public Date getTime() {
                    return new Date();
                }
            };
        }
        
        public OpflowThroughput.Source getRemoteWorkerInfoSource() {
            return new OpflowThroughput.Source() {
                @Override
                public long getValue() {
                    return remote;
                }

                @Override
                public Date getTime() {
                    return new Date();
                }
            };
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
        public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
            if (shadow != null) {
                shadow.removeComponentInstance(componentType, componentId);
            }
        }

        @Override
        public void removeComponentInstance(String componentType, String componentId) {
            if (shadow != null) {
                shadow.removeComponentInstance(componentType, componentId);
            }
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(factory, connectionType, action);
            }
        }

        @Override
        public void updateActiveChannel(String componentType, String componentId, GaugeAction action) {
            if (shadow != null) {
                shadow.updateActiveChannel(componentType, componentId, action);
            }
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineSignature, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(moduleName, eventName, routineSignature, status);
            }
            if (CONST.COMPNAME_COMMANDER.equals(moduleName)) {
                switch (eventName) {
                    case OpflowConstant.RPC_INVOCATION_FLOW_RESERVED_WORKER:
                        switch (status) {
                            case "rescue":
                                counter.incDirectRescue();
                                break;
                            case "retain":
                                counter.incDirectRetain();
                                break;
                        }
                        break;
                    case OpflowConstant.RPC_INVOCATION_FLOW_DETACHED_WORKER:
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
            }
        }
        
        @Override
        public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            return counter;
        }

        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            counter.reset();
            return counter.toMap();
        }
    }
    
    static class NullMeasurer extends OpflowPromMeasurer {

        @Override
        public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
        }

        @Override
        public void removeComponentInstance(String componentType, String componentId) {
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
        }

        @Override
        public void updateActiveChannel(String componentType, String componentId, GaugeAction action) {
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineSignature, String status) {
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
