package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowConstructorException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 *
 * @author drupalex
 */
public class OpflowExecutor {
    private final OpflowBroker broker;
    
    public OpflowExecutor(OpflowBroker engine) {
        this.broker = engine;
    }
    
    public void assertQueue(final String queueName) throws OpflowConstructorException {
        try {
            broker.checkQueue(queueName);
        } catch (IOException ioe) {
            throw new OpflowConstructorException(ioe);
        } catch (TimeoutException te) {
            throw new OpflowConstructorException(te);
        }
    }
    
    public AMQP.Queue.DeclareOk checkQueue(final String queueName) {
        try {
            return broker.checkQueue(queueName);
        } catch (Exception exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public int countQueue(final String queueName) {
        try {
            return broker.checkQueue(queueName).getMessageCount();
        } catch (Exception exception) {
            throw new OpflowOperationException(exception);
        }
    }
    
    public void purgeQueue(final String queueName) {
        try {
            broker.purgeQueue(queueName);
        } catch (Exception exception) {
            throw new OpflowOperationException(exception);
        }
    }
}
