package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
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

    private final OpflowBroker broker;
    private final OpflowExecutor executor;
    private final String operatorName;
    private final String responseName;
    
    public OpflowRpcWorker(Map<String, Object> params) throws OpflowConstructorException {
        Map<String, Object> brokerParams = new HashMap<String, Object>();
        brokerParams.put("mode", "rpc.worker");
        brokerParams.put("uri", params.get("uri"));
        brokerParams.put("exchangeName", params.get("exchangeName"));
        brokerParams.put("exchangeType", "direct");
        brokerParams.put("routingKey", params.get("routingKey"));
        brokerParams.put("applicationId", params.get("applicationId"));
        broker = new OpflowBroker(brokerParams);
        executor = new OpflowExecutor(broker);
        
        operatorName = (String) params.get("operatorName");
        if (operatorName == null) {
            throw new OpflowConstructorException("operatorName must not be null");
        } else {
            executor.assertQueue(operatorName);
        }
        
        responseName = (String) params.get("responseName");
        if (responseName != null) {
            executor.assertQueue(responseName);
        }
    }

    private OpflowBroker.ConsumerInfo consumerInfo;
    private List<Middleware> middlewares = new LinkedList<Middleware>();
    
    public OpflowBroker.ConsumerInfo process(final OpflowRpcListener listener) {
        return process(TRUE, listener);
    }

    public OpflowBroker.ConsumerInfo process(final String routineId, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineId) {
                return routineId != null && routineId.equals(originRoutineId);
            }
        }, listener);
    };
    
    public OpflowBroker.ConsumerInfo process(final String[] routineIds, final OpflowRpcListener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineId) {
                return routineIds != null && OpflowUtil.arrayContains(routineIds, originRoutineId);
            }
        }, listener);
    };
    
    public OpflowBroker.ConsumerInfo process(Checker checker, final OpflowRpcListener listener) {
        if (checker != null && listener != null) {
            middlewares.add(new Middleware(checker, listener));
        }
        if (consumerInfo != null) return consumerInfo;
        return consumerInfo = broker.consume(new OpflowListener() {
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
    
    public class State extends OpflowBroker.State {
        public State(OpflowBroker.State superState) {
            super(superState);
        }
    }
    
    public State check() {
        State state = new State(broker.check());
        return state;
    }
    
    public void close() {
        if (broker != null) {
            broker.cancelConsumer(consumerInfo);
            broker.close();
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