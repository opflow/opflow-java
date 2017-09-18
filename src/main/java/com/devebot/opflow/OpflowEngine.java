package com.devebot.opflow;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowConnectionException;
import com.devebot.opflow.exception.OpflowConsumerOverLimitException;
import com.devebot.opflow.exception.OpflowOperationException;

/**
 *
 * @author drupalex
 */
public class OpflowEngine {

    public static final String[] PARAMETER_NAMES = new String[] {
        "uri", "host", "port", "virtualHost", "username", "password", "channelMax", "frameMax", "heartbeat",
        "exchangeName", "exchangeType", "exchangeDurable", "routingKey", "otherKeys", "applicationId"
    };

    private final static Logger LOG = LoggerFactory.getLogger(OpflowEngine.class);
    private final OpflowLogTracer logTracer;
    
    private String mode;
    private ConnectionFactory factory;
    private Connection producingConnection;
    private Channel producingChannel;
    private Connection consumingConnection;
    private Channel consumingChannel;
    private List<ConsumerInfo> consumerInfos = new LinkedList<ConsumerInfo>();
    
    private String exchangeName;
    private String exchangeType;
    private Boolean exchangeDurable;
    private String routingKey;
    private String[] otherKeys;
    private String applicationId;

    public OpflowEngine(Map<String, Object> params) throws OpflowBootstrapException {
        final String engineId = OpflowUtil.getOptionField(params, "engineId", true);
        logTracer = OpflowLogTracer.ROOT.branch("engineId", engineId);
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Engine.new()")
                .toString());
        
        mode = params.containsKey("mode") ? params.get("mode").toString() : "engine";
        try {
            factory = new ConnectionFactory();
            String uri = (String) params.get("uri");
            if (uri != null) {
                factory.setUri(uri);
                if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                        .put("uri", hidePasswordInUri(uri))
                        .put("message", "Connection URI")
                        .toString());
            } else {
                String host = (String) params.get("host");
                if (host == null) host = "localhost";
                factory.setHost(host);
                
                Integer port = null;
                if (params.get("port") != null && params.get("port") instanceof Integer) {
                    factory.setPort(port = (Integer)params.get("port"));
                }
                
                String virtualHost = (String) params.get("virtualHost");
                if (virtualHost != null) {
                    factory.setVirtualHost(virtualHost);
                }
                
                String username = (String) params.get("username");
                if (username != null) {
                    factory.setUsername(username);
                }
                
                String password = (String) params.get("password");
                if (password != null) {
                    factory.setPassword(password);
                }
                
                Integer channelMax = null;
                if (params.get("channelMax") != null && params.get("channelMax") instanceof Integer) {
                    factory.setRequestedChannelMax(channelMax = (Integer)params.get("channelMax"));
                }
                
                Integer frameMax = null;
                if (params.get("frameMax") != null && params.get("frameMax") instanceof Integer) {
                    factory.setRequestedFrameMax(frameMax = (Integer)params.get("frameMax"));
                }
                
                Integer heartbeat = null;
                if (params.get("heartbeat") != null && params.get("heartbeat") instanceof Integer) {
                    factory.setRequestedHeartbeat(heartbeat = (Integer)params.get("heartbeat"));
                }
                
                if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                        .put("host", host)
                        .put("port", port)
                        .put("virtualHost", virtualHost)
                        .put("username", username)
                        .put("password", maskPassword(password))
                        .put("channelMax", channelMax)
                        .put("frameMax", frameMax)
                        .put("heartbeat", heartbeat)
                        .put("message", "Connection Parameters")
                        .toString());
            }
            this.assertConnection();
        } catch (Exception exception) {
            if (LOG.isErrorEnabled()) LOG.error("newConnection() has failed, exception: " + exception.getMessage());
            throw new OpflowConnectionException("connection refused, invalid connection parameters", exception);
        }
        
        try {
            if (params.get("exchangeName") instanceof String) {
                exchangeName = (String) params.get("exchangeName");
            }
            
            if (params.get("exchangeType") instanceof String) {
                exchangeType = (String) params.get("exchangeType");
            }
            if (exchangeType == null) exchangeType = "direct";

            if (params.get("exchangeDurable") instanceof Boolean) {
                exchangeDurable = (Boolean) params.get("exchangeDurable");
            }
            if (exchangeDurable == null) exchangeDurable = true;
            
            if (exchangeName != null) {
                getProducingChannel().exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
            }
            
            if (params.get("routingKey") instanceof String) {
                routingKey = (String) params.get("routingKey");
            }
            
            if (params.get("otherKeys") instanceof String[]) {
                otherKeys = (String[])params.get("otherKeys");
            }
            
            if (params.get("applicationId") instanceof String) {
                applicationId = (String) params.get("applicationId");
            }
            
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                        .put("exchangeName", exchangeName)
                        .put("exchangeType", exchangeType)
                        .put("exchangeDurable", exchangeDurable)
                        .put("routingKey", routingKey)
                        .put("otherKeys", otherKeys)
                        .put("applicationId", applicationId)
                        .put("message", "Exchange & routing keys")
                        .toString());
        } catch (IOException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "exchangeDeclare has failed")
                    .toString());
            throw new OpflowBootstrapException("exchangeDeclare has failed", exception);
        } catch (TimeoutException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "exchangeDeclare is timeout")
                    .toString());
            throw new OpflowBootstrapException("it maybe too slow or unstable network", exception);
        }
        
        if (LOG.isInfoEnabled()) LOG.info(logTracer
                .put("message", "Engine.new() end!")
                .toString());
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers) {
        produce(body, headers, null, null);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, AMQP.BasicProperties.Builder propBuilder) {
        produce(body, headers, propBuilder, null);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, Map<String, Object> override) {
        produce(body, headers, null, override);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, AMQP.BasicProperties.Builder propBuilder, Map<String, Object> override) {
        propBuilder = (propBuilder == null) ? new AMQP.BasicProperties.Builder() : propBuilder;
        OpflowLogTracer logProduce = null;
        
        try {
            String customKey = this.routingKey;
            if (override != null && override.get("routingKey") != null) {
                customKey = (String) override.get("routingKey");
            }
            
            String appId = this.applicationId;
            if (override != null && override.get("applicationId") != null) {
                appId = (String) override.get("applicationId");
            }
            propBuilder.appId(appId);
            
            if (override != null && override.get("correlationId") != null) {
                propBuilder.correlationId(override.get("correlationId").toString());
            }
            
            if (override != null && override.get("replyTo") != null) {
                propBuilder.replyTo(override.get("replyTo").toString());
            }
            
            String requestId = OpflowUtil.getRequestId(headers, false);
            if (requestId == null) {
                headers.put("requestId", requestId = OpflowUtil.getUUID());
            }
            propBuilder.headers(headers);
            
            if (LOG.isInfoEnabled()) logProduce = logTracer.branch("requestId", requestId);
            
            if (LOG.isInfoEnabled() && logProduce != null) LOG.info(logProduce
                    .put("appId", appId)
                    .put("customKey", customKey)
                    .put("message", "produce() is invoked")
                    .toString());
            
            Channel _channel = getProducingChannel();
            if (_channel == null || !_channel.isOpen()) {
                throw new OpflowOperationException("Channel is null or has been closed");
            }
            _channel.basicPublish(this.exchangeName, customKey, propBuilder.build(), body);
        } catch (IOException exception) {
            if (LOG.isErrorEnabled() && logProduce != null) LOG.error(logProduce.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "produce() has failed")
                    .toString());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (LOG.isErrorEnabled() && logProduce != null) LOG.error(logProduce.reset()
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .put("message", "produce() is timeout")
                    .toString());
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowListener listener, final Map<String, Object> options) {
        final Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        final String _consumerId = OpflowUtil.getOptionField(opts, "consumerId", true);
        final OpflowLogTracer logConsume = logTracer.branch("consumerId", _consumerId);
        
        if (LOG.isInfoEnabled()) LOG.info(logConsume
                .put("message", "consume() is invoked")
                .toString());
        try {
            final boolean _forceNewConnection = Boolean.TRUE.equals(opts.get("forceNewConnection"));
            final Boolean _forceNewChannel = Boolean.TRUE.equals(opts.get("forceNewChannel"));
            final Channel _channel = getConsumingChannel(_forceNewConnection, _forceNewChannel);
            final Connection _connection = _channel.getConnection();
            
            Integer _prefetch = null;
            if (opts.get("prefetch") instanceof Integer) {
                _prefetch = (Integer) opts.get("prefetch");
            }
            if (_prefetch != null && _prefetch > 0) {
                _channel.basicQos(_prefetch);
            }
            
            final String _queueName;
            final boolean _fixedQueue;
            String opts_queueName = (String) opts.get("queueName");
            AMQP.Queue.DeclareOk _declareOk;
            if (opts_queueName != null) {
                _declareOk = _channel.queueDeclare(opts_queueName, true, false, false, null);
                _fixedQueue = true;
            } else {
                _declareOk = _channel.queueDeclare();
                _fixedQueue = false;
            }
            _queueName = _declareOk.getQueue();
            final Integer _consumerLimit = (Integer) opts.get("consumerLimit");
            if (LOG.isTraceEnabled()) LOG.trace(logConsume.reset()
                    .put("consumerCount", _declareOk.getConsumerCount())
                    .put("consumerLimit", _consumerLimit)
                    .put("message", "consume() - consumerCount/consumerLimit")
                    .toString());
            if (_consumerLimit != null && _consumerLimit > 0) {
                if (_declareOk.getConsumerCount() >= _consumerLimit) {
                    if (LOG.isErrorEnabled()) LOG.error(logConsume.reset()
                            .put("consumerCount", _declareOk.getConsumerCount())
                            .put("consumerLimit", _consumerLimit)
                            .put("message", "consume() - consumerCount exceed limit")
                            .toString());
                    String errorMessage = "consumerLimit exceed: " + _declareOk.getConsumerCount() + "/" + _consumerLimit;
                    throw new OpflowConsumerOverLimitException(errorMessage);
                }
            }
            
            final Boolean _binding = (Boolean) opts.get("binding");
            if (!Boolean.FALSE.equals(_binding) && exchangeName != null) {
                if (routingKey != null) {
                    bindExchange(_channel, exchangeName, _queueName, routingKey);
                }
                if (otherKeys != null) {
                    bindExchange(_channel, exchangeName, _queueName, otherKeys);
                }
            }
            
            final String _replyToName;
            String opts_replyToName = (String) opts.get("replyTo");
            if (opts_replyToName != null) {
                _replyToName = _channel.queueDeclarePassive(opts_replyToName).getQueue();
            } else {
                _replyToName = null;
            }
            
            final Boolean _autoAck;
            if (opts.get("autoAck") != null && opts.get("autoAck") instanceof Boolean) {
                _autoAck = (Boolean) opts.get("autoAck");
            } else {
                _autoAck = Boolean.TRUE;
            }
            
            final Consumer _consumer = new DefaultConsumer(_channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    final String requestID = OpflowUtil.getRequestId(properties.getHeaders(), false);
                    
                    final OpflowLogTracer logRequest = logConsume.branch("requestId", requestID);
                    
                    if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                            .put("appId", properties.getAppId())
                            .put("deliveryTag", envelope.getDeliveryTag())
                            .put("consumerTag", consumerTag)
                            .put("message", "consumer received a message")
                            .toString());
                    
                    if (LOG.isTraceEnabled()) {
                        if (body.length <= 4096) {
                            if (LOG.isTraceEnabled()) LOG.trace(logRequest.reset()
                                    .put("bodyHead", new String(body, "UTF-8"))
                                    .put("bodyLength", body.length)
                                    .put("message", "Body head (4096 bytes)")
                                    .toString());
                        } else {
                            if (LOG.isTraceEnabled()) LOG.trace(logRequest.reset()
                                    .put("bodyLength", body.length)
                                    .put("message", "Body size too large (>4KB)")
                                    .toString());
                        }
                    }
                    
                    try {
                        if (applicationId == null || applicationId.equals(properties.getAppId())) {
                            if (LOG.isTraceEnabled()) LOG.trace(logRequest.reset()
                                    .put("message", "Request invoke listener.processMessage()")
                                    .toString());
                            
                            boolean captured = listener.processMessage(body, properties, _replyToName, _channel, consumerTag);
                            
                            if (captured) {
                                if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                                        .put("message", "Request has finished successfully")
                                        .toString());
                            } else {
                                if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                                        .put("message", "Request has not matched the criteria, skipped")
                                        .toString());
                            }
                            
                            if (LOG.isTraceEnabled()) LOG.trace(logRequest.reset()
                                    .put("deliveryTag", envelope.getDeliveryTag())
                                    .put("consumerTag", consumerTag)
                                    .put("message", "Request invoke ACK")
                                    .toString());
                            
                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                                    .put("message", "Request has been rejected, mismatched applicationId")
                                    .put("applicationId", applicationId)
                                    .toString());
                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    } catch (Exception ex) {
                        // catch ALL of Error here: don't let it harm our service/close the channel
                        if (LOG.isErrorEnabled()) LOG.error(logRequest.reset()
                                .put("deliveryTag", envelope.getDeliveryTag())
                                .put("consumerTag", consumerTag)
                                .put("exceptionClass", ex.getClass().getName())
                                .put("exceptionMessage", ex.getMessage())
                                .put("message", "Request has been failed. Service still alive")
                                .toString());
                        if (_autoAck) {
                            if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                                    .put("message", "Request has failed. AutoAck => request is rejected")
                                    .toString());
                        } else {
                            _channel.basicNack(envelope.getDeliveryTag(), false, true);
                            if (LOG.isInfoEnabled()) LOG.info(logRequest.reset()
                                    .put("message", "Request has failed. No AutoAck => request is requeued")
                                    .toString());
                        }
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (LOG.isInfoEnabled()) LOG.info(logConsume.reset()
                            .put("consumerTag", consumerTag)
                            .put("message", "consume() - handle CancelOk event")
                            .toString());
                }
                
                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (LOG.isInfoEnabled()) LOG.info(logConsume.reset()
                            .put("consumerTag", consumerTag)
                            .put("message", "consume() - handle ShutdownSignal event")
                            .toString());
                }
            };
            
            final String _consumerTag = _channel.basicConsume(_queueName, _autoAck, _consumer);
            
            _channel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (LOG.isInfoEnabled()) LOG.info(logConsume.reset()
                            .put("queueName", _queueName)
                            .put("consumerTag", _consumerTag)
                            .put("channelNumber", _channel.getChannelNumber())
                            .put("message", "consume() channel has been shutdown successfully")
                            .toString());
                }
            });
            
            if (LOG.isInfoEnabled()) LOG.info(logConsume.reset()
                    .put("queueName", _queueName)
                    .put("consumerTag", _consumerTag)
                    .put("channelNumber", _channel.getChannelNumber())
                    .put("message", "consume() consume the queue")
                    .toString());
            ConsumerInfo info = new ConsumerInfo(_connection, !_forceNewConnection, 
                    _channel, !_forceNewChannel, _queueName, _fixedQueue, _consumerId, _consumerTag);
            if ("engine".equals(mode)) consumerInfos.add(info);
            return info;
        } catch(IOException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logConsume.reset()
                    .put("message", "consume() - has failed")
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .toString());
            throw new OpflowOperationException(exception);
        } catch(TimeoutException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logConsume.reset()
                    .put("message", "consume() - is timeout")
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .toString());
            throw new OpflowOperationException(exception);
        }
    }
    
    public interface Operator {
        public Object handleEvent(Channel channel) throws IOException;
    }
    
    public <T> T acquireChannel(Operator listener) throws IOException, TimeoutException {
        T output = null;
        Connection _connection = null;
        Channel _channel = null;
        try {
            _connection = factory.newConnection();
            _channel = _connection.createChannel();
            if (listener != null) output = (T) listener.handleEvent(_channel);
        } finally {
            if (_channel != null && _channel.isOpen()) _channel.close();
            if (_connection != null && _connection.isOpen()) _connection.close();
        }
        return output;
    }
    
    public void cancelConsumer(OpflowEngine.ConsumerInfo consumerInfo) {
        if (consumerInfo == null) return;
        final OpflowLogTracer logCancel = logTracer.branch("consumerId", consumerInfo.getConsumerId());
        try {
            if (LOG.isDebugEnabled()) LOG.debug(logCancel.reset()
                    .put("queueName", consumerInfo.getQueueName())
                    .put("message", "cancelConsumer() - consumer will be cancelled")
                    .toString());

            consumerInfo.getChannel().basicCancel(consumerInfo.getConsumerTag());

            if (LOG.isDebugEnabled()) LOG.debug(logCancel.reset()
                    .put("message", "cancelConsumer() - consumer has been cancelled")
                    .toString());

            if (!consumerInfo.isSharedConnection() || !consumerInfo.isSharedChannel()) {
                if (consumerInfo.getChannel() != null && consumerInfo.getChannel().isOpen()) {
                    if (LOG.isDebugEnabled()) LOG.debug(logCancel.reset()
                            .put("message", "cancelConsumer() - close private channel")
                            .toString());
                    consumerInfo.getChannel().close();
                }
            }

            if (!consumerInfo.isSharedConnection()) {
                if (consumerInfo.getConnection() != null && consumerInfo.getConnection().isOpen()) {
                    if (LOG.isDebugEnabled()) LOG.debug(logCancel.reset()
                            .put("message", "cancelConsumer() - close private connection")
                            .toString());
                    consumerInfo.getConnection().close();
                }
            }
        } catch (IOException ex) {
            if (LOG.isErrorEnabled()) LOG.error(logCancel.reset()
                    .put("message", "cancelConsumer() - has failed")
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionMessage", ex.getMessage())
                    .toString());
        } catch (TimeoutException ex) {
            if (LOG.isErrorEnabled()) LOG.error(logCancel.reset()
                    .put("message", "cancelConsumer() - is timeout")
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionMessage", ex.getMessage())
                    .toString());
        }
    }
    
    public class ConsumerInfo {
        private final Connection connection;
        private final boolean sharedConnection;
        private final Channel channel;
        private final boolean sharedChannel;
        private final String queueName;
        private final boolean fixedQueue;
        private final String consumerId;
        private final String consumerTag;
        
        public ConsumerInfo(Connection connection, boolean sharedConnection, Channel channel, boolean sharedChannel, 
                String queueName, boolean fixedQueue, String consumerId, String consumerTag) {
            this.connection = connection;
            this.sharedConnection = sharedConnection;
            this.channel = channel;
            this.sharedChannel = sharedChannel;
            this.queueName = queueName;
            this.fixedQueue = fixedQueue;
            this.consumerId = consumerId;
            this.consumerTag = consumerTag;
        }

        public Connection getConnection() {
            return connection;
        }

        public boolean isSharedConnection() {
            return sharedConnection;
        }

        public Channel getChannel() {
            return channel;
        }

        public boolean isSharedChannel() {
            return sharedChannel;
        }

        public String getQueueName() {
            return queueName;
        }

        public boolean isFixedQueue() {
            return fixedQueue;
        }
        
        public String getConsumerId() {
            return consumerId;
        }

        public String getConsumerTag() {
            return consumerTag;
        }
    }
    
    public static class State {
        public static final int CONNECTION_NEW = 0;
        public static final int CONNECTION_OPENED = 1;
        public static final int CONNECTION_CLOSED = 2;
        private final int[] CONNECTION_STATES =  new int[] {
            CONNECTION_NEW, CONNECTION_OPENED, CONNECTION_CLOSED
        };
        
        private int connectionState = -1;
        
        public int getConnectionState() {
            return connectionState;
        }
        
        public State(State state) {
            this.connectionState = state.connectionState;
        }
        
        private State(int connectionState) {
            for (int i=0; i<CONNECTION_STATES.length; i++) {
                if (CONNECTION_STATES[i] == connectionState) {
                    this.connectionState = connectionState;
                    break;
                }
            }
            if (this.connectionState < 0) this.connectionState = CONNECTION_NEW;
        }
    }
    
    public State check() {
        int conn = producingConnection.isOpen() ? State.CONNECTION_OPENED : State.CONNECTION_CLOSED;
        State state = new State(conn);
        return state;
    }
    
    /**
     * Close this broker.
     *
     * @throws OpflowOperationException if an error is encountered
     */
    public void close() {
        try {
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close() - close producingChannel, producingConnection")
                .toString());
            if (producingChannel != null && producingChannel.isOpen()) producingChannel.close();
            if (producingConnection != null && producingConnection.isOpen()) producingConnection.close();
            
            if ("engine".equals(mode)) {
                if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                        .put("mode", mode)
                        .put("message", "close() - cancel consumers")
                        .toString());
                for(ConsumerInfo consumerInfo: consumerInfos) {
                    this.cancelConsumer(consumerInfo);
                }
                consumerInfos.clear();
            }
            
            if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                .put("message", "close() - close consumingChannel, consumingConnection")
                .toString());
            if (consumingChannel != null && consumingChannel.isOpen()) consumingChannel.close();
            if (consumingConnection != null && consumingConnection.isOpen()) consumingConnection.close();

        } catch (IOException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("message", "close() has failed")
                    .toString());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (LOG.isErrorEnabled()) LOG.error(logTracer.reset()
                    .put("message", "close() is timeout")
                    .toString());
            throw new OpflowOperationException(exception);
        }
    }
    
    private void assertConnection() throws IOException, TimeoutException {
        this.acquireChannel(new Operator() {
            @Override
            public Object handleEvent(Channel channel) throws IOException {
                return null; // try to connection
            }
        });
    }
    
    private Connection getProducingConnection() throws IOException, TimeoutException {
        if (producingConnection == null || !producingConnection.isOpen()) {
            producingConnection = factory.newConnection();
        }
        return producingConnection;
    }
    
    private Channel getProducingChannel() throws IOException, TimeoutException {
        if (producingChannel == null || !producingChannel.isOpen()) {
            producingChannel = getProducingConnection().createChannel();
            producingChannel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                            .put("channelNumber", producingChannel.getChannelNumber())
                            .put("message", "producingChannel has been shutdown")
                            .toString());
                }
            });
        }
        return producingChannel;
    }
    
    private Connection getConsumingConnection(boolean forceNewConnection) throws IOException, TimeoutException {
        if (forceNewConnection) {
            return factory.newConnection();
        }
        if (consumingConnection == null || !consumingConnection.isOpen()) {
            consumingConnection = factory.newConnection();
        }
        return consumingConnection;
    }
    
    private Channel getConsumingChannel(boolean forceNewConnection, boolean forceNewChannel) throws IOException, TimeoutException {
        if (forceNewConnection) {
            return getConsumingConnection(forceNewConnection).createChannel();
        }
        if (forceNewChannel) {
            return getConsumingConnection(false).createChannel();
        }
        if (consumingChannel == null || !consumingChannel.isOpen()) {
            consumingChannel = getConsumingConnection(false).createChannel();
            consumingChannel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (LOG.isInfoEnabled()) LOG.info(logTracer.reset()
                            .put("channelNumber", consumingChannel.getChannelNumber())
                            .put("message", "consumingChannel has been shutdown")
                            .toString());
                }
            });
        }
        return consumingChannel;
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String _routingKey) throws IOException {
        bindExchange(_channel, _exchangeName, _queueName, new String[] { _routingKey });
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String[] keys) throws IOException {
        _channel.exchangeDeclarePassive(_exchangeName);
        _channel.queueDeclarePassive(_queueName);
        for (String _routingKey : keys) {
            _channel.queueBind(_queueName, _exchangeName, _routingKey);
            if (LOG.isTraceEnabled()) LOG.trace(logTracer.reset()
                    .put("exchangeName", _exchangeName)
                    .put("queueName", _queueName)
                    .put("routingKey", _routingKey)
                    .put("message", "Binds Exchange to Queue")
                    .toString());
        }
    }
    
    private Pattern passwordPattern = Pattern.compile(":([^:]+)@");
    
    private String maskPassword(String password) {
        if (password == null) return null;
        char[] charArray = new char[password.length()];
        Arrays.fill(charArray, '*');
        return new String(charArray);
    }
    
    private String hidePasswordInUri(String uri) {
        return passwordPattern.matcher(uri).replaceAll(":******@");
    }
}