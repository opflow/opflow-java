package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.nostro.client.AMQP;
import com.rabbitmq.nostro.client.Channel;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author drupalex
 */
public class OpflowExecutor {
    private final OpflowEngine engine;
    
    public OpflowExecutor(OpflowEngine engine) {
        this.engine = engine;
    }
    
    public void assertQueue(final String queueName) throws OpflowBootstrapException {
        assertQueue(queueName, null, null, null, null);
    }
    
    public void assertQueue(
            final String queueName,
            Boolean durable,
            Boolean exclusive,
            Boolean autoDelete
    ) throws OpflowBootstrapException {
        assertQueue(queueName, durable, exclusive, autoDelete, null);
    }
    
    public void assertQueue(
            final String queueName,
            Boolean durable,
            Boolean exclusive,
            Boolean autoDelete,
            Map<String, Object> options
    ) throws OpflowBootstrapException {
        try {
            if (durable == null) durable = true;
            if (exclusive == null) exclusive = false;
            if (autoDelete == null) autoDelete = false;
            declareQueue(queueName, durable, exclusive, autoDelete, options);
        } catch (IOException | TimeoutException ioe) {
            throw new OpflowBootstrapException(ioe);
        }
    }
    
    public AMQP.Queue.DeclareOk defineQueue(final String queueName) {
        return defineQueue(queueName, null, null, null);
    }
    
    public AMQP.Queue.DeclareOk defineQueue(final String queueName, Boolean durable, Boolean exclusive, Boolean autoDelete) {
        try {
            durable = (durable == null) ? true : durable;
            exclusive = (exclusive == null) ? false : exclusive;
            autoDelete = (autoDelete == null) ? false : autoDelete;
            return declareQueue(queueName, durable, exclusive, autoDelete, null);
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    private AMQP.Queue.DeclareOk declareQueue(
            final String queueName,
            final boolean durable,
            final boolean exclusive,
            final boolean autoDelete,
            final Map<String, Object> options
    ) throws IOException, TimeoutException {
        if (queueName == null) return null;
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public AMQP.Queue.DeclareOk handleEvent(Channel _channel) throws IOException {
                    return _channel.queueDeclarePassive(queueName);
                }
            });
        } catch (IOException e1) {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public AMQP.Queue.DeclareOk handleEvent(Channel _channel) throws IOException {
                    return _channel.queueDeclare(queueName, durable, exclusive, autoDelete, options);
                }
            });
        }
    }
    
    public int countQueue(final String queueName) {
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public Integer handleEvent(Channel _channel) throws IOException {
                    AMQP.Queue.DeclareOk ok = _channel.queueDeclarePassive(queueName);
                    return ok.getMessageCount();
                }
            });
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public AMQP.Queue.PurgeOk purgeQueue(final String queueName) {
        if (queueName == null) return null;
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public AMQP.Queue.PurgeOk handleEvent(Channel _channel) throws IOException {
                    return _channel.queuePurge(queueName);
                }
            });
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public AMQP.Queue.DeleteOk deleteQueue(final String queueName) {
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public Object handleEvent(Channel channel) throws IOException {
                    return channel.queueDelete(queueName, true, false);
                }
            });
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public AMQP.Exchange.DeclareOk defineExchange(final String exchangeName) {
        return defineExchange(exchangeName, null);
    }
    
    public AMQP.Exchange.DeclareOk defineExchange(final String exchangeName, final String exchangeType) {
        try {
            return declareExchange(exchangeName, exchangeType, true, false, null);
        } catch (IOException | TimeoutException ioe) {
            throw new OpflowOperationException(ioe);
        }
    }
    
    private AMQP.Exchange.DeclareOk declareExchange(final String exchangeName,
            final String exchangeType,
            final boolean durable,
            final boolean autoDelete,
            final Map<String, Object> options
    ) throws IOException, TimeoutException {
        if (exchangeName == null) return null;
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public AMQP.Exchange.DeclareOk handleEvent(Channel _channel) throws IOException {
                    return _channel.exchangeDeclarePassive(exchangeName);
                }
            });
        } catch (IOException e1) {
            final String _type = (exchangeType != null) ? exchangeType : "direct";
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public AMQP.Exchange.DeclareOk handleEvent(Channel _channel) throws IOException {
                    return _channel.exchangeDeclare(exchangeName, _type, durable, autoDelete, options);
                }
            });
        }
    }
    
    public AMQP.Exchange.DeleteOk deleteExchange(final String exchangeName) {
        try {
            return engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public Object handleEvent(Channel channel) throws IOException {
                    return channel.exchangeDelete(exchangeName);
                }
            });
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public void bindExchange(final String exchangeName, final String routingKey, final String queueName) {
        bindExchange(exchangeName, new String[] { routingKey }, queueName);
    }
    
    public void bindExchange(final String exchangeName, final String[] bindingKeys, final String queueName) {
        try {
            engine.acquireChannel(new OpflowEngine.Operator() {
                @Override
                public Object handleEvent(Channel channel) throws IOException {
                    channel.exchangeDeclarePassive(exchangeName);
                    channel.queueDeclarePassive(queueName);
                    for (String bindingKey : bindingKeys) {
                        channel.queueBind(queueName, exchangeName, bindingKey);
                    }
                    return null;
                }
            });
        } catch (IOException | TimeoutException exception) {
            throw new OpflowOperationException(exception);
        }
    }
}
