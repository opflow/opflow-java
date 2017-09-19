package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowRpcWorker {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcWorker.class);
    private final OpflowLogTracer logTracer;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws OpflowBootstrapException {
        final String rpcWorkerId = OpflowUtil.getOptionField(params, "rpcWorkerId", true);
        logTracer = OpflowLogTracer.ROOT.branch("rpcWorkerId", rpcWorkerId);
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "RpcWorker.new()")
                .toString());
        
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("engineId", rpcWorkerId);
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
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("operatorName", operatorName)
                .put("responseName", responseName)
                .put("message", "RpcWorker.new() parameters")
                .toString());
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "RpcWorker.new() end!")
                .toString());
    }

    private OpflowEngine.ConsumerInfo consumerInfo;
    private List<Middleware> middlewares = new LinkedList<Middleware>();
    
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
    
    public OpflowEngine.ConsumerInfo process(Checker checker, final OpflowRpcListener listener) {
        final String _consumerId = OpflowUtil.getUUID();
        final OpflowLogTracer logProcess = logTracer.branch("consumerId", _consumerId);
        if (LOG.isInfoEnabled()) LOG.info(logProcess
                .put("message", "process() is invoked")
                .toString());
        
        if (checker != null && listener != null) {
            middlewares.add(new Middleware(checker, listener));
        }
        if (consumerInfo != null) return consumerInfo;
        consumerInfo = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] body, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                OpflowMessage request = new OpflowMessage(body, properties.getHeaders());
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, workerTag, queueName);
                String routineId = OpflowUtil.getRoutineId(properties.getHeaders(), false);
                String requestId = OpflowUtil.getRequestId(properties.getHeaders(), false);

                OpflowLogTracer logRequest = null;
                if (LOG.isInfoEnabled()) logRequest = logProcess.branch("requestId", requestId);

                if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest
                        .put("routineId", routineId)
                        .put("message", "process() - receives a new RPC request")
                        .toString());
                int count = 0;
                for(Middleware middleware : middlewares) {
                    if (middleware.getChecker().match(routineId)) {
                        count++;
                        Boolean nextAction = middleware.getListener().processMessage(request, response);
                        if (nextAction == null || nextAction == OpflowRpcListener.DONE) break;
                    }
                }
                if (LOG.isInfoEnabled() && logRequest != null) LOG.info(logRequest.reset()
                        .put("message", "process() - RPC request processing has completed")
                        .toString());
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
        if (LOG.isInfoEnabled()) LOG.info(logProcess.reset()
                .put("message", "process() has completed")
                .toString());
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
    
    public void close() {
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close()")
                .toString());
        if (engine != null) {
            engine.cancelConsumer(consumerInfo);
            engine.close();
        }
        if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close() has completed")
                .toString());
    }

    public OpflowExecutor getExecutor() {
        return executor;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public String getResponseName() {
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
}