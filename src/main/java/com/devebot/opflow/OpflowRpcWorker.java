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

    final Logger logger = LoggerFactory.getLogger(OpflowRpcWorker.class);

    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws OpflowBootstrapException {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);
        brokerParams.put("mode", "rpc.worker");
        brokerParams.put("exchangeType", "direct");
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        operatorName = (String) params.get("operatorName");
        if (operatorName == null) {
            throw new OpflowBootstrapException("operatorName must not be null");
        } else {
            executor.assertQueue(operatorName);
        }
        
        responseName = (String) params.get("responseName");
        if (responseName != null) {
            executor.assertQueue(responseName);
        }
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
        if (checker != null && listener != null) {
            middlewares.add(new Middleware(checker, listener));
        }
        if (consumerInfo != null) return consumerInfo;
        return consumerInfo = engine.consume(new OpflowListener() {
            @Override
            public boolean processMessage(byte[] content, AMQP.BasicProperties properties, 
                    String queueName, Channel channel, String workerTag) throws IOException {
                OpflowMessage request = new OpflowMessage(content, properties.getHeaders());
                OpflowRpcResponse response = new OpflowRpcResponse(channel, properties, workerTag, queueName);
                String routineId = OpflowUtil.getRoutineId(properties.getHeaders(), false);
                int count = 0;
                for(Middleware middleware : middlewares) {
                    if (middleware.getChecker().match(routineId)) {
                        count++;
                        Boolean nextAction = middleware.getListener().processMessage(request, response);
                        if (nextAction == null || nextAction == OpflowRpcListener.DONE) break;
                    }
                }
                return count > 0;
            }
        }, OpflowUtil.buildOptions(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                opts.put("queueName", operatorName);
                opts.put("replyTo", responseName);
                opts.put("binding", Boolean.TRUE);
            }
        }));
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
        if (engine != null) {
            engine.cancelConsumer(consumerInfo);
            engine.close();
        }
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