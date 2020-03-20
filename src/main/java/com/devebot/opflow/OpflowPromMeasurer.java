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
    public static final String LABEL_RPC_DIRECT_WORKER = "rpcOverNativeWorker";
    public static final String LABEL_RPC_REMOTE_AMQP_WORKER = "rpcOverRemoteAMQPWorkers";
    public static final String LABEL_RPC_REMOTE_HTTP_WORKER = "rpcOverRemoteHTTPWorkers";
    
    public static enum GaugeAction { INC, DEC }
    
    public abstract void updateComponentInstance(String componentType, String componentId, GaugeAction action);
    
    public abstract void updateEngineConnection(ConnectionFactory factory, String connectionOwner, String connectionType, GaugeAction action);
    
    public abstract void countRpcInvocation(String componentType, String eventName, String routineSignature, String status);
    
    public abstract RpcInvocationCounter getRpcInvocationCounter(String componentType);
    
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
        private long remoteAMQPTotal = 0;
        private long remoteAMQPSuccess = 0;
        private long remoteAMQPFailure = 0;
        private long remoteAMQPTimeout = 0;
        private long remoteHTTPTotal = 0;
        private long remoteHTTPSuccess = 0;
        private long remoteHTTPFailure = 0;
        private long remoteHTTPTimeout = 0;

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

        public synchronized void incRemoteAMQPSuccess() {
            this.total++;
            this.remoteAMQPTotal++;
            this.remoteAMQPSuccess++;
        }

        public synchronized void incRemoteAMQPFailure() {
            this.total++;
            this.remoteAMQPTotal++;
            this.remoteAMQPFailure++;
        }

        public synchronized void incRemoteAMQPTimeout() {
            this.total++;
            this.remoteAMQPTotal++;
            this.remoteAMQPTimeout++;
        }
        
        public synchronized void incRemoteHTTPSuccess() {
            this.total++;
            this.remoteHTTPTotal++;
            this.remoteHTTPSuccess++;
        }

        public synchronized void incRemoteHTTPFailure() {
            this.total++;
            this.remoteHTTPTotal++;
            this.remoteHTTPFailure++;
        }

        public synchronized void incRemoteHTTPTimeout() {
            this.total++;
            this.remoteHTTPTotal++;
            this.remoteHTTPTimeout++;
        }

        private synchronized RpcInvocationCounter copy() {
            RpcInvocationCounter that = new RpcInvocationCounter();
            that.startTime = this.startTime;
            that.total = this.total;
            that.direct = this.direct;
            that.directRescue = this.directRescue;
            that.directRetain = this.directRetain;
            that.remoteAMQPTotal = this.remoteAMQPTotal;
            that.remoteAMQPSuccess = this.remoteAMQPSuccess;
            that.remoteAMQPFailure = this.remoteAMQPFailure;
            that.remoteAMQPTimeout = this.remoteAMQPTimeout;
            that.remoteHTTPTotal = this.remoteHTTPTotal;
            that.remoteHTTPSuccess = this.remoteHTTPSuccess;
            that.remoteHTTPFailure = this.remoteHTTPFailure;
            that.remoteHTTPTimeout = this.remoteHTTPTimeout;
            return that;
        }

        public synchronized void reset() {
            this.startTime = new Date();
            this.total = 0;
            this.direct = 0;
            this.directRescue = 0;
            this.directRetain = 0;
            // AMQP workers
            this.remoteAMQPTotal = 0;
            this.remoteAMQPSuccess = 0;
            this.remoteAMQPFailure = 0;
            this.remoteAMQPTimeout = 0;
            // HTTP workers
            this.remoteHTTPTotal = 0;
            this.remoteHTTPSuccess = 0;
            this.remoteHTTPFailure = 0;
            this.remoteHTTPTimeout = 0;
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
                    .put(LABEL_RPC_REMOTE_AMQP_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            opts.put("total", that.remoteAMQPTotal);
                            opts.put("ok", that.remoteAMQPSuccess);
                            opts.put("failed", that.remoteAMQPFailure);
                            opts.put("timeout", that.remoteAMQPTimeout);
                            if (verbose) {
                                double remoteRate = calcMessageRate(that.remoteAMQPTotal, elapsedTime);
                                opts.put("rate", formatMessageRate(remoteRate));
                                opts.put("rateNumber", remoteRate);
                            }
                        }
                    }).toMap())
                    .put(LABEL_RPC_REMOTE_HTTP_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                        @Override
                        public void transform(Map<String, Object> opts) {
                            opts.put("total", that.remoteHTTPTotal);
                            opts.put("ok", that.remoteHTTPSuccess);
                            opts.put("failed", that.remoteHTTPFailure);
                            opts.put("timeout", that.remoteHTTPTimeout);
                            if (verbose) {
                                double remoteRate = calcMessageRate(that.remoteHTTPTotal, elapsedTime);
                                opts.put("rate", formatMessageRate(remoteRate));
                                opts.put("rateNumber", remoteRate);
                            }
                        }
                    }).toMap())
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
                    .put("startTime", that.startTime)
                    .put("elapsedTime", OpflowDateTime.printElapsedTime(that.startTime, currentTime))
                    .toMap();
        }
        
        public OpflowThroughput.Source getNativeWorkerInfoSource() {
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
        
        public OpflowThroughput.Source getRemoteAMQPWorkerInfoSource() {
            return new OpflowThroughput.Source() {
                @Override
                public long getValue() {
                    return remoteAMQPTotal;
                }

                @Override
                public Date getTime() {
                    return new Date();
                }
            };
        }
        
        public OpflowThroughput.Source getRemoteHTTPWorkerInfoSource() {
            return new OpflowThroughput.Source() {
                @Override
                public long getValue() {
                    return remoteHTTPTotal;
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
                shadow.updateComponentInstance(componentType, componentId, action);
            }
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionOwner, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(factory, connectionOwner, connectionType, action);
            }
        }

        @Override
        public void countRpcInvocation(String componentType, String eventName, String routineSignature, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(componentType, eventName, routineSignature, status);
            }
            if (OpflowConstant.COMP_COMMANDER.equals(componentType)) {
                switch (eventName) {
                    case OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER:
                        switch (status) {
                            case "rescue":
                                counter.incDirectRescue();
                                break;
                            case "retain":
                                counter.incDirectRetain();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER:
                        switch (status) {
                            case "ok":
                                counter.incRemoteAMQPSuccess();
                                break;
                            case "failed":
                                counter.incRemoteAMQPFailure();
                                break;
                            case "timeout":
                                counter.incRemoteAMQPTimeout();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER:
                        switch (status) {
                            case "ok":
                                counter.incRemoteHTTPSuccess();
                                break;
                            case "failed":
                                counter.incRemoteHTTPFailure();
                                break;
                            case "timeout":
                                counter.incRemoteHTTPTimeout();
                                break;
                        }
                        break;
                    default:
                        break;
                }
            }
        }
        
        @Override
        public RpcInvocationCounter getRpcInvocationCounter(String componentType) {
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
        public void updateEngineConnection(ConnectionFactory factory, String connectionOwner, String connectionType, GaugeAction action) {
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
