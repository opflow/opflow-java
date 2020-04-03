package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowMathUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPromMeasurer.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();
    
    public static final String LABEL_RPC_INVOCATION_TOTAL = "rpcInvocationTotal";
    public static final String LABEL_RPC_ACCEPTED_INVOCATION_TOTAL = "rpcProcessingTotal";
    public static final String LABEL_RPC_REJECTED_INVOCATION_TOTAL = "rpcRejectedTotal";
    public static final String LABEL_RPC_REJECTED_INVOCATION_DETAILS = "rpcRejectedDetails";
    public static final String LABEL_RPC_CANCELLATION_TOTAL = "rpcCancellationTotal";
    public static final String LABEL_RPC_SERVICE_NOT_READY_TOTAL = "rpcServiceNotReadyTotal";
    public static final String LABEL_RPC_PAUSING_TIMEOUT_TOTAL = "rpcPausingTimeoutTotal";
    public static final String LABEL_RPC_SEMAPHORE_TIMEOUT_TOTAL = "rpcSemaphoreTimeoutTotal";
    public static final String LABEL_RPC_PUBLISHER = "rpcOverPublisher";
    public static final String LABEL_RPC_DIRECT_WORKER = "rpcOverNativeWorker";
    public static final String LABEL_RPC_REMOTE_AMQP_WORKER = "rpcOverRemoteAMQPWorkers";
    public static final String LABEL_RPC_REMOTE_HTTP_WORKER = "rpcOverRemoteHTTPWorkers";
    
    public static enum GaugeAction { INC, DEC }
    
    public abstract void updateComponentInstance(String componentType, String componentId, GaugeAction action);
    
    public abstract void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action);
    
    public abstract void countRpcInvocation(String componentType, String eventName, String routineSignature, String status);
    
    public abstract RpcInvocationCounter getRpcInvocationCounter(String componentType);
    
    public abstract Map<String, Object> resetRpcInvocationCounter();
    
    public abstract Map<String, Object> getServiceInfo();
    
    public static Class<? extends OpflowPromMeasurer> PromExporter;
    
    private static PipeMeasurer instance = new PipeMeasurer();
    
    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return instance;
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (OpflowUtil.isComponentEnabled(kwargs)) {
            if (!instance.hasShadow()) {
                synchronized (OpflowPromMeasurer.class) {
                    if (!instance.hasShadow()) {
                        if (PromExporter != null) {
                            try {
                                instance.setShadow((OpflowPromMeasurer) PromExporter.getDeclaredConstructor(Map.class).newInstance(kwargs));
                                if (LOG_TRACER.ready(LOG, Level.DEBUG)) LOG.debug(LOG_TRACER
                                        .put("className", PromExporter.getName())
                                        .text("Measurer[${instanceId}].getInstance() - create an object [${className}] and assign it to the measurer")
                                        .stringify());
                            }
                            catch (NoSuchMethodException | SecurityException ex) {
                                if (LOG_TRACER.ready(LOG, Level.ERROR)) LOG.error(LOG_TRACER
                                        .put("className", ex.getClass().getName())
                                        .put("message", ex.getMessage())
                                        .text("Measurer[${instanceId}].getInstance() - getDeclaredConstructor() - exception[${className}]: ${message}")
                                        .stringify());
                            }
                            catch (IllegalAccessException | IllegalArgumentException | InstantiationException | InvocationTargetException ex) {
                                if (LOG_TRACER.ready(LOG, Level.ERROR)) LOG.error(LOG_TRACER
                                        .put("className", ex.getClass().getName())
                                        .put("message", ex.getMessage())
                                        .text("Measurer[${instanceId}].getInstance() - newInstance() - exception[${className}]: ${message}")
                                        .stringify());
                            }
                        }
                    }
                }
            }
        }
        return instance;
    }
    
    public static class RpcInvocationCounter {
        private Date startTime = new Date();
        private volatile long total = 0;
        // Restrictor
        private volatile long acceptedRpcTotal = 0;
        private volatile long rejectedRpcTotal = 0;
        private volatile long cancellationRpcTotal = 0;
        private volatile long serviceNotReadyRpcTotal = 0;
        private volatile long pausingTimeoutRpcTotal = 0;
        private volatile long semaphoreTimeoutRpcTotal = 0;
        // Publisher
        private volatile long publishingTotal = 0;
        // Native worker
        private volatile long direct = 0;
        private volatile long directRetain = 0;
        private volatile long directRescue = 0;
        // AMQP workers
        private volatile long remoteAMQPTotal = 0;
        private volatile long remoteAMQPSuccess = 0;
        private volatile long remoteAMQPFailure = 0;
        private volatile long remoteAMQPTimeout = 0;
        // HTTP workers
        private volatile long remoteHTTPTotal = 0;
        private volatile long remoteHTTPSuccess = 0;
        private volatile long remoteHTTPFailure = 0;
        private volatile long remoteHTTPTimeout = 0;

        private boolean publisherEnabled = false;
        private boolean nativeWorkerEnabled = false;
        private boolean remoteAMQPWorkerEnabled = false;
        private boolean remoteHTTPWorkerEnabled = false;
        
        public RpcInvocationCounter() {
        }

        public synchronized void incRejectedRpc() {
            this.total++;
            this.rejectedRpcTotal++;
        }
        
        public synchronized void incCancellationRpc() {
            this.total++;
            this.rejectedRpcTotal++;
            this.cancellationRpcTotal++;
        }
        
        public synchronized void incServiceNotReadyRpc() {
            this.total++;
            this.rejectedRpcTotal++;
            this.serviceNotReadyRpcTotal++;
        }
        
        public synchronized void incPausingTimeoutRpc() {
            this.total++;
            this.rejectedRpcTotal++;
            this.pausingTimeoutRpcTotal++;
        }
        
        public synchronized void incSemaphoreTimeoutRpc() {
            this.total++;
            this.rejectedRpcTotal++;
            this.semaphoreTimeoutRpcTotal++;
        }
        
        public synchronized void incPublishingOk() {
            this.total++;
            this.acceptedRpcTotal++;
            this.publishingTotal++;
        }
        
        public synchronized void incDirectRescue() {
            this.total++;
            this.acceptedRpcTotal++;
            this.direct++;
            this.directRescue++;
        }
        
        public synchronized void incDirectRetain() {
            this.total++;
            this.acceptedRpcTotal++;
            this.direct++;
            this.directRetain++;
        }

        public synchronized void incRemoteAMQPSuccess() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteAMQPTotal++;
            this.remoteAMQPSuccess++;
        }

        public synchronized void incRemoteAMQPFailure() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteAMQPTotal++;
            this.remoteAMQPFailure++;
        }

        public synchronized void incRemoteAMQPTimeout() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteAMQPTotal++;
            this.remoteAMQPTimeout++;
        }
        
        public synchronized void incRemoteHTTPSuccess() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteHTTPTotal++;
            this.remoteHTTPSuccess++;
        }

        public synchronized void incRemoteHTTPFailure() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteHTTPTotal++;
            this.remoteHTTPFailure++;
        }

        public synchronized void incRemoteHTTPTimeout() {
            this.total++;
            this.acceptedRpcTotal++;
            this.remoteHTTPTotal++;
            this.remoteHTTPTimeout++;
        }

        private synchronized RpcInvocationCounter copy() {
            RpcInvocationCounter that = new RpcInvocationCounter();
            that.startTime = this.startTime;
            that.total = this.total;
            // Restrictor
            that.acceptedRpcTotal = this.acceptedRpcTotal;
            that.rejectedRpcTotal = this.rejectedRpcTotal;
            that.cancellationRpcTotal = this.cancellationRpcTotal;
            that.serviceNotReadyRpcTotal = this.serviceNotReadyRpcTotal;
            that.pausingTimeoutRpcTotal = this.pausingTimeoutRpcTotal;
            that.semaphoreTimeoutRpcTotal = this.semaphoreTimeoutRpcTotal;
            // Publisher
            that.publishingTotal = this.publishingTotal;
            // Native worker
            that.direct = this.direct;
            that.directRescue = this.directRescue;
            that.directRetain = this.directRetain;
            // AMQP workers
            that.remoteAMQPTotal = this.remoteAMQPTotal;
            that.remoteAMQPSuccess = this.remoteAMQPSuccess;
            that.remoteAMQPFailure = this.remoteAMQPFailure;
            that.remoteAMQPTimeout = this.remoteAMQPTimeout;
            // HTTP workers
            that.remoteHTTPTotal = this.remoteHTTPTotal;
            that.remoteHTTPSuccess = this.remoteHTTPSuccess;
            that.remoteHTTPFailure = this.remoteHTTPFailure;
            that.remoteHTTPTimeout = this.remoteHTTPTimeout;
            return that;
        }

        public synchronized void reset() {
            this.startTime = new Date();
            this.total = 0;
            // Restrictor
            this.acceptedRpcTotal = 0;
            this.rejectedRpcTotal = 0;
            this.cancellationRpcTotal = 0;
            this.serviceNotReadyRpcTotal = 0;
            this.pausingTimeoutRpcTotal = 0;
            this.semaphoreTimeoutRpcTotal = 0;
            // Publisher
            this.publishingTotal = 0;
            // Native worker
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
            
            OpflowObjectTree.Builder builder = OpflowObjectTree.buildMap()
                    .put(LABEL_RPC_INVOCATION_TOTAL, that.total);
            
            if (that.acceptedRpcTotal < that.total) {
                builder = builder.put(LABEL_RPC_ACCEPTED_INVOCATION_TOTAL, that.acceptedRpcTotal)
                    .put(LABEL_RPC_REJECTED_INVOCATION_TOTAL, that.rejectedRpcTotal)
                    .put(LABEL_RPC_REJECTED_INVOCATION_DETAILS, OpflowObjectTree.buildMap()
                        .put(LABEL_RPC_CANCELLATION_TOTAL, that.cancellationRpcTotal, that.cancellationRpcTotal > 0)
                        .put(LABEL_RPC_SERVICE_NOT_READY_TOTAL, that.serviceNotReadyRpcTotal, that.serviceNotReadyRpcTotal > 0)
                        .put(LABEL_RPC_PAUSING_TIMEOUT_TOTAL, that.pausingTimeoutRpcTotal, that.pausingTimeoutRpcTotal > 0)
                        .put(LABEL_RPC_SEMAPHORE_TIMEOUT_TOTAL, that.semaphoreTimeoutRpcTotal, that.semaphoreTimeoutRpcTotal > 0)
                        .toMap());
            }
            
            if (publisherEnabled) {
                builder.put(LABEL_RPC_PUBLISHER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        opts.put("total", that.publishingTotal);
                        if (verbose) {
                            double remoteRate = calcMessageRate(that.publishingTotal, elapsedTime);
                            opts.put("rate", formatMessageRate(remoteRate));
                            opts.put("rateNumber", remoteRate);
                        }
                    }
                }).toMap());
            }
            
            if (remoteAMQPWorkerEnabled) {
                builder.put(LABEL_RPC_REMOTE_AMQP_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
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
                }).toMap());
            }
            
            if (remoteHTTPWorkerEnabled) {
                builder.put(LABEL_RPC_REMOTE_HTTP_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
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
                }).toMap());
            }
            
            if (nativeWorkerEnabled) {
                builder.put(LABEL_RPC_DIRECT_WORKER, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
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
                }).toMap());
            }
            
            return builder
                    .put(OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP, that.startTime)
                    .put(OpflowConstant.OPFLOW_COMMON_ELAPSED_TIME, OpflowDateTime.printElapsedTime(that.startTime, currentTime))
                    .toMap();
        }
        
        public OpflowThroughput.Source getPublisherInfoSource() {
            return new OpflowThroughput.Source() {
                @Override
                public long getValue() {
                    return publishingTotal;
                }

                @Override
                public Date getTime() {
                    return new Date();
                }
            };
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

        public void setPublisherEnabled(boolean enabled) {
            this.publisherEnabled = enabled;
        }

        public void setNativeWorkerEnabled(boolean enabled) {
            this.nativeWorkerEnabled = enabled;
        }

        public void setRemoteAMQPWorkerEnabled(boolean enabled) {
            this.remoteAMQPWorkerEnabled = enabled;
        }

        public void setRemoteHTTPWorkerEnabled(boolean enabled) {
            this.remoteHTTPWorkerEnabled = enabled;
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
        public void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(connectionOwner, connectionType, action);
            }
        }

        @Override
        public void countRpcInvocation(String componentType, String eventName, String routineSignature, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(componentType, eventName, routineSignature, status);
            }
            if (OpflowConstant.COMP_COMMANDER.equals(componentType)) {
                switch (eventName) {
                    case OpflowConstant.METHOD_INVOCATION_FLOW_RESTRICTOR:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_REJECTED:
                                counter.incRejectedRpc();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_FLOW_PUBSUB:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_ENTER:
                                counter.incPublishingOk();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_RESCUE:
                                counter.incDirectRescue();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_NORMAL:
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

        @Override
        public Map<String, Object> getServiceInfo() {
            if (shadow != null) {
                return shadow.getServiceInfo();
            }
            return null;
        }
    }
    
    static class NullMeasurer extends OpflowPromMeasurer {

        @Override
        public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
        }

        @Override
        public void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action) {
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

        @Override
        public Map<String, Object> getServiceInfo() {
            return null;
        }
    }
    
    public static final OpflowPromMeasurer NULL = new NullMeasurer();
}
