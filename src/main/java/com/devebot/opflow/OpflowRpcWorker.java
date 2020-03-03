package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowObjectTree;
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
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWorker.class);
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final Integer prefetchCount;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowObjectTree.ensureNonNull(params);
        
        componentId = OpflowUtil.getOptionField(params, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, CONST.COMPNAME_MEASURER, OpflowPromMeasurer.NULL);
        
        logTracer = OpflowLogTracer.ROOT.branch("rpcWorkerId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}][${instanceId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put(CONST.COMPONENT_ID, componentId);
        brokerParams.put(CONST.COMPNAME_MEASURER, measurer);
        brokerParams.put("mode", "rpc_worker");
        brokerParams.put("exchangeType", "direct");
        
        operatorName = (String) params.get("operatorName");
        responseName = (String) params.get("responseName");
        
        if (operatorName != null && responseName != null && operatorName.equals(responseName)) {
            throw new OpflowBootstrapException("operatorName should be different with responseName");
        }
        
        if (params.get("prefetchCount") != null && params.get("prefetchCount") instanceof Integer) {
            prefetchCount = (Integer) params.get("prefetchCount");
        } else {
            prefetchCount = null;
        }
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        if (operatorName != null) {
            executor.assertQueue(operatorName);
        }
        
        if (responseName != null) {
            executor.assertQueue(responseName);
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("operatorName", operatorName)
                .put("responseName", responseName)
                .tags("RpcWorker.new() parameters")
                .text("RpcWorker[${rpcWorkerId}].new() operatorName: '${operatorName}', responseName: '${responseName}'")
                .stringify());
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}][${instanceId}].new() end!")
                .stringify());

        measurer.updateComponentInstance("rpc_worker", componentId, OpflowPromMeasurer.GaugeAction.INC);
    }

    private OpflowEngine.ConsumerInfo consumerInfo;
    private List<Middleware> middlewares = new LinkedList<>();
    
    public OpflowEngine.ConsumerInfo process(final OpflowRpcListener listener) {
        return process(TRUE, listener);
    }

    public OpflowEngine.ConsumerInfo process(final String routineSignature, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignature != null && routineSignature.equals(originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final String[] routineSignatures, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignatures != null && OpflowUtil.arrayContains(routineSignatures, originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final Set<String> routineSignatures, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignatures != null && routineSignatures.contains(originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(Checker checker, final OpflowRpcListener listener) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logProcess = logTracer.branch("consumerId", _consumerId);
        if (logProcess.ready(LOG, Level.INFO)) LOG.info(logProcess
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
                    String consumerTag,
                    Map<String, String> extras
            ) throws IOException {
                Map<String, Object> headers = properties.getHeaders();
                OpflowMessage request = new OpflowMessage(body, headers);
                
                if (extras == null) {
                    extras = new HashMap<>();
                }
                
                String routineId = extras.get(CONST.AMQP_HEADER_ROUTINE_ID);
                String routineTimestamp = extras.get(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP);
                String routineScope = extras.get(CONST.AMQP_HEADER_ROUTINE_SCOPE);
                
                if (routineId == null) routineId = OpflowUtil.getRoutineId(headers);
                if (routineTimestamp == null) routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
                if (routineScope == null) routineScope = OpflowUtil.getRoutineScope(headers);
                
                String routineSignature = OpflowUtil.getRoutineSignature(headers, false);
                
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, componentId, consumerTag, queueName,
                        routineId, routineTimestamp, routineScope, routineSignature);
                
                OpflowLogTracer reqTracer = null;
                if (logProcess.ready(LOG, Level.INFO)) {
                    reqTracer = logProcess.branch(CONST.REQUEST_TIME, routineTimestamp)
                            .branch(CONST.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));
                }
                
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .put("routineSignature", routineSignature)
                        .text("Request[${requestId}][${requestTime}][x-rpc-worker-request-received] - Consumer[${consumerId}] receives a new RPC [${routineSignature}]")
                        .stringify());
                int count = 0;
                for(Middleware middleware : middlewares) {
                    if (middleware.getChecker().match(routineSignature)) {
                        count++;
                        measurer.countRpcInvocation("rpc_worker", componentId, routineSignature, "process");
                        Boolean nextAction = middleware.getListener().processMessage(request, response);
                        if (nextAction == null || nextAction == OpflowRpcListener.DONE) break;
                    }
                }
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-rpc-worker-request-finished] - RPC request processing has completed")
                        .stringify());
                return count > 0;
            }
        }, OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_ID, _consumerId);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME, operatorName);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_REPLY_TO, responseName);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_AUTO_BINDING, Boolean.TRUE);
                if (prefetchCount != null) {
                    opts.put(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT, prefetchCount);
                }
            }
        }).toMap());
        if (logProcess.ready(LOG, Level.INFO)) LOG.info(logProcess
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
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}][${instanceId}].close()")
                .stringify());
        if (engine != null) {
            engine.cancelConsumer(consumerInfo);
            engine.close();
        }
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("RpcWorker[${rpcWorkerId}][${instanceId}].close() end!")
                .stringify());
    }

    public OpflowEngine getEngine() {
        return engine;
    }

    public OpflowExecutor getExecutor() {
        return executor;
    }

    public String getComponentId() {
        return componentId;
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
        public boolean match(String routineSignature);
    }
    
    private final Checker TRUE = new Checker() {
        @Override
        public boolean match(String routineSignature) {
            return true;
        }
    };
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance("rpc_worker", componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
