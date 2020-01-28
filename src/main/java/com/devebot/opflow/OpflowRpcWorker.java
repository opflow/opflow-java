package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcWorker implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWorker.class);
    
    private final String instanceId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowUtil.ensureNotNull(params);
        
        instanceId = OpflowUtil.getOptionField(params, "instanceId", true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, "measurer", OpflowPromMeasurer.NULL);
        
        logTracer = OpflowLogTracer.ROOT.branch("rpcWorkerId", instanceId);
        
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("instanceId", instanceId);
        brokerParams.put("measurer", measurer);
        brokerParams.put("mode", "rpc_worker");
        brokerParams.put("exchangeType", "direct");
        
        operatorName = (String) params.get("operatorName");
        responseName = (String) params.get("responseName");
        
        if (operatorName != null && responseName != null && operatorName.equals(responseName)) {
            throw new OpflowBootstrapException("operatorName should be different with responseName");
        }
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        if (operatorName != null) {
            executor.assertQueue(operatorName);
        }
        
        if (responseName != null) {
            executor.assertQueue(responseName);
        }
        
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .put("operatorName", operatorName)
                .put("responseName", responseName)
                .tags("RpcWorker.new() parameters")
                .text("RpcWorker[${rpcWorkerId}].new() operatorName: '${operatorName}', responseName: '${responseName}'")
                .stringify());
        
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}].new() end!")
                .stringify());
    
        measurer.updateComponentInstance("rpc_worker", instanceId, OpflowPromMeasurer.GaugeAction.INC);
    }

    private OpflowEngine.ConsumerInfo consumerInfo;
    private List<Middleware> middlewares = new LinkedList<>();
    
    public OpflowEngine.ConsumerInfo process(final OpflowRpcListener listener) {
        return process(TRUE, listener);
    }

    public OpflowEngine.ConsumerInfo process(final String routineId, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineId) {
                return routineId != null && routineId.equals(originRoutineId);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final String[] routineIds, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineId) {
                return routineIds != null && OpflowUtil.arrayContains(routineIds, originRoutineId);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final Set<String> routineIds, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineId) {
                return routineIds != null && routineIds.contains(originRoutineId);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(Checker checker, final OpflowRpcListener listener) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logProcess = logTracer.branch("consumerId", _consumerId);
        if (logProcess.ready(LOG, "info")) LOG.info(logProcess
                .text("Consumer[${consumerId}] - RpcWorker[${rpcWorkerId}].process() is invoked")
                .stringify());
        
        if (checker != null && listener != null) {
            middlewares.add(new Middleware(checker, listener));
        }
        if (consumerInfo != null) return consumerInfo;
        consumerInfo = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(
                    byte[] body,
                    AMQP.BasicProperties properties,
                    String queueName,
                    Channel channel,
                    String consumerTag
            ) throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                OpflowMessage request = new OpflowMessage(body, headers);
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, consumerTag, queueName);
                String routineId = OpflowUtil.getRoutineId(headers, false);
                String requestTime = OpflowUtil.getRequestTime(headers, false);
                String requestId = OpflowUtil.getRequestId(headers, false);

                OpflowLogTracer logRequest = null;
                if (logProcess.ready(LOG, "info")) {
                    logRequest = logProcess.branch("requestTime", requestTime)
                            .branch("requestId", requestId, new OpflowLogTracer.OmitPingLogs(headers));
                }

                if (logRequest != null && logRequest.ready(LOG, "info")) LOG.info(logRequest
                        .put("routineId", routineId)
                        .text("Request[${requestId}] - Consumer[${consumerId}] receives a new RPC request")
                        .stringify());
                int count = 0;
                for(Middleware middleware : middlewares) {
                    if (middleware.getChecker().match(routineId)) {
                        count++;
                        measurer.countRpcInvocation("rpc_worker", instanceId, routineId, "process");
                        Boolean nextAction = middleware.getListener().processMessage(request, response);
                        if (nextAction == null || nextAction == OpflowRpcListener.DONE) break;
                    }
                }
                if (logRequest != null && logRequest.ready(LOG, "info")) LOG.info(logRequest
                        .text("Request[${requestId}] - RPC request processing has completed")
                        .stringify());
                return count > 0;
            }
        }, OpflowUtil.buildMap(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("consumerId", _consumerId);
                opts.put("queueName", operatorName);
                opts.put("replyTo", responseName);
                opts.put("binding", Boolean.TRUE);
            }
        }).toMap());
        if (logProcess.ready(LOG, "info")) LOG.info(logProcess
                .text("Consumer[${consumerId}] - process() has completed")
                .stringify());
        return consumerInfo;
    }
    
    public class State extends OpflowEngine.State {
        public State(OpflowEngine.State superState) {
            super(superState);
        }
    }
    
    public State check() {
        State state = new State(engine.check());
        return state;
    }
    
    @Override
    public void close() {
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}].close()")
                .stringify());
        if (engine != null) {
            engine.cancelConsumer(consumerInfo);
            engine.close();
        }
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}].close() has completed")
                .stringify());
    }

    public OpflowExecutor getExecutor() {
        return executor;
    }

    public OpflowEngine getEngine() {
        return engine;
    }

    public String getIntanceId() {
        return instanceId;
    }
    
    public String getDispatchName() {
        return operatorName;
    }

    public String getCallbackName() {
        return responseName;
    }
    
    public class Middleware {
        private final Checker checker;
        private final OpflowRpcListener listener;

        public Middleware(Checker checker, OpflowRpcListener listener) {
            this.checker = checker;
            this.listener = listener;
        }

        public Checker getChecker() {
            return checker;
        }

        public OpflowRpcListener getListener() {
            return listener;
        }
    }
    
    public interface Checker {
        public boolean match(String routineId);
    }
    
    private final Checker TRUE = new Checker() {
        @Override
        public boolean match(String routineId) {
            return true;
        }
    };
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance("rpc_worker", instanceId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
