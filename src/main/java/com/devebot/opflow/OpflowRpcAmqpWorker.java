package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowCollectionUtil;
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
public class OpflowRpcAmqpWorker implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcAmqpWorker.class);
    
    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    
    private final OpflowEngine engine;
    private final OpflowExecutor executor;
    
    private final String dispatchExchangeName;
    private final String dispatchRoutingKey;
    
    private final String[] incomingBindingKeys;
    private final String incomingQueueName;
    private final Boolean incomingQueueAutoDelete;
    private final Boolean incomingQueueDurable;
    private final Boolean incomingQueueExclusive;
    private final Integer incomingPrefetchCount;
    
    private final String responseQueueName;
    
    private String httpAddress = null;
    
    public OpflowRpcAmqpWorker(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowObjectTree.ensureNonNull(params);
        
        componentId = OpflowUtil.getStringField(params, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, OpflowConstant.COMP_MEASURER, OpflowPromMeasurer.NULL);
        
        logTracer = OpflowLogTracer.ROOT.branch("amqpWorkerId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("amqpWorker[${amqpWorkerId}][${instanceId}].new()")
                .stringify());
        
        Map<String, Object> brokerParams = new HashMap<>();

        OpflowUtil.copyParameters(brokerParams, params, OpflowEngine.PARAMETER_NAMES);

        brokerParams.put(CONST.COMPONENT_ID, componentId);
        brokerParams.put(OpflowConstant.COMP_MEASURER, measurer);
        brokerParams.put(OpflowConstant.OPFLOW_COMMON_INSTANCE_OWNER, OpflowConstant.COMP_RPC_AMQP_WORKER);

        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME, params.get(OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_NAME));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_TYPE, params.get(OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_TYPE));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE, params.get(OpflowConstant.OPFLOW_OUTGOING_EXCHANGE_DURABLE));
        brokerParams.put(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY, params.get(OpflowConstant.OPFLOW_OUTGOING_ROUTING_KEY));

        // Use for autoBinding
        if (params.get(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME) instanceof String) {
            dispatchExchangeName = (String) params.get(OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME);
        } else {
            dispatchExchangeName = null;
        }
        
        if (params.get(OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY) instanceof String) {
            dispatchRoutingKey = (String) params.get(OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY);
        } else {
            dispatchRoutingKey = null;
        }
        
        incomingQueueName = (String) params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME);
        
        if (incomingQueueName == null) {
            throw new OpflowBootstrapException("incomingQueueName must not be null");
        }
        
        if (params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE) instanceof Boolean) {
            incomingQueueAutoDelete = (Boolean) params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_AUTO_DELETE);
        } else {
            incomingQueueAutoDelete = null;
        }
        
        if (params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE) instanceof Boolean) {
            incomingQueueDurable = (Boolean) params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_DURABLE);
        } else {
            incomingQueueDurable = null;
        }
        
        if (params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE) instanceof Boolean) {
            incomingQueueExclusive = (Boolean) params.get(OpflowConstant.OPFLOW_INCOMING_QUEUE_EXCLUSIVE);
        } else {
            incomingQueueExclusive = null;
        }
        
        if (params.get(OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS) instanceof String[]) {
            incomingBindingKeys = (String[])params.get(OpflowConstant.OPFLOW_INCOMING_BINDING_KEYS);
        } else {
            incomingBindingKeys = null;
        }
        
        if (params.get(OpflowConstant.OPFLOW_INCOMING_PREFETCH_COUNT) instanceof Integer) {
            incomingPrefetchCount = (Integer) params.get(OpflowConstant.OPFLOW_INCOMING_PREFETCH_COUNT);
        } else {
            incomingPrefetchCount = null;
        }
        
        engine = new OpflowEngine(brokerParams);
        executor = new OpflowExecutor(engine);
        
        if (incomingQueueName != null) {
            executor.assertQueue(incomingQueueName, incomingQueueDurable, incomingQueueExclusive, incomingQueueAutoDelete);
        }
        
        // responseQueue - Deprecated
        responseQueueName = (String) params.get(OpflowConstant.OPFLOW_RESPONSE_QUEUE_NAME);
        
        if (incomingQueueName != null && responseQueueName != null && incomingQueueName.equals(responseQueueName)) {
            throw new OpflowBootstrapException("incomingQueueName should be different with responseQueueName");
        }
        
        if (responseQueueName != null) {
            executor.assertQueue(responseQueueName);
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("queueName", incomingQueueName)
                .tags("RpcAmqpWorker.new() parameters")
                .text("amqpWorker[${amqpWorkerId}].new() queueName: '${queueName}'")
                .stringify());
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("amqpWorker[${amqpWorkerId}][${instanceId}].new() end!")
                .stringify());

        measurer.updateComponentInstance(OpflowConstant.COMP_RPC_AMQP_WORKER, componentId, OpflowPromMeasurer.GaugeAction.INC);
    }

    private OpflowEngine.ConsumerInfo consumerInfo;
    private List<Middleware> middlewares = new LinkedList<>();
    
    public OpflowEngine.ConsumerInfo process(final Listener listener) {
        return process(TRUE, listener);
    }

    public OpflowEngine.ConsumerInfo process(final String routineSignature, final Listener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignature != null && routineSignature.equals(originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final String[] routineSignatures, final Listener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignatures != null && OpflowCollectionUtil.arrayContains(routineSignatures, originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(final Set<String> routineSignatures, final Listener listener) {
        return process(new Checker() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignatures != null && routineSignatures.contains(originRoutineSignature);
            }
        }, listener);
    };
    
    public OpflowEngine.ConsumerInfo process(Checker checker, final Listener listener) {
        final String _consumerId = OpflowUUID.getBase64ID();
        final OpflowLogTracer logProcess = logTracer.branch("consumerId", _consumerId);
        if (logProcess.ready(LOG, Level.INFO)) LOG.info(logProcess
                .text("Consumer[${consumerId}] - amqpWorker[${amqpWorkerId}].process() is invoked")
                .stringify());
        
        if (checker != null && listener != null) {
            middlewares.add(new Middleware(checker, listener));
        }
        if (consumerInfo != null) return consumerInfo;
        consumerInfo = engine.consume(new OpflowEngine.Listener() {
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
                OpflowEngine.Message request = new OpflowEngine.Message(body, headers);
                
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
                
                OpflowRpcAmqpResponse response = new OpflowRpcAmqpResponse(channel, properties, componentId, consumerTag, queueName,
                        routineId, routineTimestamp, routineScope, routineSignature, httpAddress);
                
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
                        measurer.countRpcInvocation(OpflowConstant.COMP_RPC_AMQP_WORKER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "process");
                        Boolean nextAction = middleware.getListener().processMessage(request, response);
                        if (nextAction == null || nextAction == Listener.DONE) break;
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
                opts.put(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME, dispatchExchangeName);
                opts.put(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY, dispatchRoutingKey);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_ID, _consumerId);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME, incomingQueueName);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_AUTO_DELETE, incomingQueueAutoDelete);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_DURABLE, incomingQueueDurable);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_QUEUE_EXCLUSIVE, incomingQueueExclusive);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_BINDING_KEYS, incomingBindingKeys);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_REPLY_TO, responseQueueName);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_AUTO_BINDING, Boolean.TRUE);
                opts.put(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT, incomingPrefetchCount);
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
                .text("amqpWorker[${amqpWorkerId}][${instanceId}].close()")
                .stringify());
        if (engine != null) {
            engine.cancelConsumer(consumerInfo);
            engine.close();
        }
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("amqpWorker[${amqpWorkerId}][${instanceId}].close() end!")
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
    
    public String getIncomingQueueName() {
        return incomingQueueName;
    }

    public Boolean getIncomingQueueDurable() {
        return incomingQueueDurable;
    }

    public Boolean getIncomingQueueExclusive() {
        return incomingQueueExclusive;
    }

    public Boolean getIncomingQueueAutoDelete() {
        return incomingQueueAutoDelete;
    }

    public void setHttpAddress(String address) {
        this.httpAddress = address;
    }
    
    public class Middleware {
        private final Checker checker;
        private final Listener listener;

        public Middleware(Checker checker, Listener listener) {
            this.checker = checker;
            this.listener = listener;
        }

        public Checker getChecker() {
            return checker;
        }

        public Listener getListener() {
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
    
    public interface Listener {
        public static final Boolean DONE = Boolean.FALSE;
        public static final Boolean NEXT = Boolean.TRUE;
        public Boolean processMessage(OpflowEngine.Message message, OpflowRpcAmqpResponse response) throws IOException;
    }
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_RPC_AMQP_WORKER, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
}
