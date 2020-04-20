package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowMathUtil;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author acegik
 */
public class OpflowRpcInvocationCounter {
    
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

    public OpflowRpcInvocationCounter() {
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

    private synchronized OpflowRpcInvocationCounter copy() {
        OpflowRpcInvocationCounter that = new OpflowRpcInvocationCounter();
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
        final OpflowRpcInvocationCounter that = cloned ? this.copy() : this;
        final Date currentTime = new Date();
        final long elapsedTime = (currentTime.getTime() - that.startTime.getTime());

        OpflowObjectTree.Builder builder = OpflowObjectTree.buildMap()
                .put(LABEL_RPC_INVOCATION_TOTAL, that.total);

        if (that.rejectedRpcTotal > 0) {
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
