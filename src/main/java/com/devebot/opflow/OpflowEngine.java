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
import java.text.MessageFormat;
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

    private final Logger logger = LoggerFactory.getLogger(OpflowEngine.class);

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
        mode = params.containsKey("mode") ? params.get("mode").toString() : "engine";
        try {
            factory = new ConnectionFactory();

            String uri = (String) params.get("uri");
            if (uri != null) {
                factory.setUri(uri);
                if (logger.isTraceEnabled()) logger.trace("Connection parameter/URI: " + hidePasswordInUri(uri));
            } else {
                String host = (String) params.get("host");
                if (host == null) host = "localhost";
                factory.setHost(host);
                if (logger.isTraceEnabled()) logger.trace("Connection parameter/host: " + host);

                if (params.get("port") != null && params.get("port") instanceof Integer) {
                    Integer port;
                    factory.setPort(port = (Integer)params.get("port"));
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/port: " + port);
                }

                String virtualHost = (String) params.get("virtualHost");
                if (virtualHost != null) {
                    factory.setVirtualHost(virtualHost);
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/virtualHost: " + virtualHost);
                }

                String username = (String) params.get("username");
                if (username != null) {
                    factory.setUsername(username);
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/username: " + username);
                }

                String password = (String) params.get("password");
                if (password != null) {
                    factory.setPassword(password);
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/password: ******");
                }

                if (params.get("channelMax") != null && params.get("channelMax") instanceof Integer) {
                    Integer channelMax;
                    factory.setRequestedChannelMax(channelMax = (Integer)params.get("channelMax"));
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/channelMax: " + channelMax);
                }

                if (params.get("frameMax") != null && params.get("frameMax") instanceof Integer) {
                    Integer frameMax;
                    factory.setRequestedFrameMax(frameMax = (Integer)params.get("frameMax"));
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/frameMax: " + frameMax);
                }

                if (params.get("heartbeat") != null && params.get("heartbeat") instanceof Integer) {
                    Integer heartbeat;
                    factory.setRequestedHeartbeat(heartbeat = (Integer)params.get("heartbeat"));
                    if (logger.isTraceEnabled()) logger.trace("Connection parameter/heartbeat: " + heartbeat);
                }
            }
            this.assertConnection();
        } catch (Exception exception) {
            if (logger.isErrorEnabled()) logger.error("newConnection() has failed, exception: " + exception.getMessage());
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
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("exchangeDeclare has failed, exception: " + exception.getMessage());
            throw new OpflowBootstrapException("exchangeDeclare has failed", exception);
        } catch (TimeoutException exception) {
            if (logger.isErrorEnabled()) logger.error("connection is timeout, exception: " + exception.getMessage());
            throw new OpflowBootstrapException("it maybe too slow or unstable network", exception);
        }
    }
    
    public void produce(final byte[] content, final AMQP.BasicProperties.Builder propBuilder) {
        produce(content, propBuilder, null);
    }
    
    public void produce(final byte[] content, final AMQP.BasicProperties.Builder propBuilder, final Map<String, Object> override) {
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
            Channel _channel = getProducingChannel();
            if (_channel == null || !_channel.isOpen()) {
                throw new OpflowOperationException("Channel is null or has been closed");
            }
            _channel.basicPublish(this.exchangeName, customKey, propBuilder.build(), content);
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("produce() has failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (logger.isErrorEnabled()) logger.error("produce() is timeout, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowListener listener, final Map<String, Object> options) {
        Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
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
            if (logger.isTraceEnabled()) {
                logger.trace("consume() - consumerCount/consumerLimit: " + _declareOk.getConsumerCount() + "/" + _consumerLimit);
            }
            if (_consumerLimit != null && _consumerLimit > 0) {
                if (_declareOk.getConsumerCount() >= _consumerLimit) {
                    String errorMessage = "consumerLimit exceed: " + _declareOk.getConsumerCount() + "/" + _consumerLimit;
                    if (logger.isErrorEnabled()) logger.error("consume() - " + errorMessage);
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
                    String requestID = OpflowUtil.getRequestId(properties.getHeaders(), false);

                    if (logger.isInfoEnabled()) {
                        logger.info("Request["+requestID+"] / DeliveryTag["+envelope.getDeliveryTag()+"] / ConsumerTag["+consumerTag+"]");
                    }

                    if (logger.isTraceEnabled()) {
                        if (body.length <= 4096) {
                            logger.trace("Request[" + requestID + "] - Message: " + new String(body, "UTF-8"));
                        } else {
                            logger.trace("Request[" + requestID + "] - Message size too large (>4KB): " + body.length);
                        }
                    }
                    
                    try {
                        if (applicationId == null || applicationId.equals(properties.getAppId())) {
                            if (logger.isTraceEnabled()) {
                                logger.trace(MessageFormat.format("Request[{0}] invoke listener.processMessage()", new Object[] {
                                    requestID
                                }));
                            }

                            boolean captured = listener.processMessage(body, properties, _replyToName, _channel, consumerTag);

                            if (captured) {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Request[" + requestID + "] has finished successfully");
                                }
                            } else {
                                if (logger.isInfoEnabled()) {
                                    logger.info("Request[" + requestID + "] has not matched the criteria, skipped");
                                }
                            }
                            
                            if (logger.isTraceEnabled()) {
                                logger.trace(MessageFormat.format("Request[{0}] invoke Ack({1}, false)) / ConsumerTag[{2}]", new Object[] {
                                    requestID, envelope.getDeliveryTag(), consumerTag
                                }));
                            }

                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            if (logger.isInfoEnabled()) {
                                logger.info(MessageFormat.format("Request[{0}]/AppId:{1} - but received AppId:{2}, rejected", new Object[] {
                                    requestID, applicationId, properties.getAppId()
                                }));
                            }
                            if (!_autoAck) _channel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    } catch (Exception ex) {
                        // catch ALL of Error here: don't let it harm our service/close the channel
                        if (logger.isErrorEnabled()) {
                            logger.error(MessageFormat.format("Request[{0}]/DeliveryTag[{1}]/ConsumerTag[{2}] has been failed. " +
                                    "Exception.Class: {3} / message: {4}. Service still alive", new Object[] {
                                requestID, envelope.getDeliveryTag(), consumerTag, ex.getClass().getName(), ex.getMessage()
                            }));
                        }
                        if (_autoAck) {
                            if (logger.isInfoEnabled()) {
                                logger.info("Request[" + requestID + "] has been failed. AutoAck => request is rejected");
                            }
                        } else {
                            _channel.basicNack(envelope.getDeliveryTag(), false, true);
                            if (logger.isInfoEnabled()) {
                                logger.info("Request[" + requestID + "] has been failed. No AutoAck => request is requeued");
                            }
                        }
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (!Boolean.FALSE.equals(_forceNewChannel)) {
                        try {
                            if (_channel != null && _channel.isOpen()) _channel.close();
                        } catch (IOException ex) {
                            if (logger.isErrorEnabled()) {
                                logger.error(MessageFormat.format("ConsumerTag[{0}] handleCancelOk failed, IOException: {1}", new Object[] {
                                    consumerTag, ex.getMessage()
                                }));
                            }
                        } catch (TimeoutException ex) {
                            if (logger.isErrorEnabled()) {
                                logger.error(MessageFormat.format("ConsumerTag[{0}] handleCancelOk failed, TimeoutException: {1}", new Object[] {
                                    consumerTag, ex.getMessage()
                                }));
                            }
                        } catch (ShutdownSignalException sig) {
                            if (logger.isErrorEnabled()) {
                                logger.error(MessageFormat.format("ConsumerTag[{0}] handleCancelOk failed, ShutdownSignalException: {1}", new Object[] {
                                    consumerTag, sig.getMessage()
                                }));
                            }
                        }
                    }
                }
                
                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (logger.isInfoEnabled()) {
                        logger.info(MessageFormat.format("ConsumerTag[{0}] handle shutdown signal", new Object[] {
                            consumerTag
                        }));
                    }
                }
            };
            
            final String _consumerTag = _channel.basicConsume(_queueName, _autoAck, _consumer);
            
            _channel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (logger.isInfoEnabled()) {
                        logger.info(MessageFormat.format("Channel[{0}] contains Queue[{1}]/ConsumerTag[{2}] has been shutdown", new Object[] {
                            _channel.getChannelNumber(), _queueName, _consumerTag
                        }));
                    }
                }
            });
            
            if (logger.isInfoEnabled()) {
                logger.info("[*] Consume Channel[" + _channel.getChannelNumber() + "]/Queue[" + _queueName + "] -> consumerTag: " + _consumerTag);
            }
            ConsumerInfo info = new ConsumerInfo(_connection, !_forceNewConnection, _channel, !_forceNewChannel, _queueName, _fixedQueue, _consumer, _consumerTag);
            if ("engine".equals(mode)) consumerInfos.add(info);
            return info;
        } catch(IOException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() has failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        } catch(TimeoutException exception) {
            if (logger.isErrorEnabled()) logger.error("consume() is timeout, exception: " + exception.getMessage());
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
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Queue[" + consumerInfo.getQueueName() + "]/ConsumerTag[" + consumerInfo.getConsumerTag() + "] will be cancelled");
            }
            consumerInfo.getChannel().basicCancel(consumerInfo.getConsumerTag());
            if (logger.isDebugEnabled()) {
                logger.debug("Queue[" + consumerInfo.getQueueName() + "]/ConsumerTag[" + consumerInfo.getConsumerTag() + "] has been cancelled");
            }
            if (!consumerInfo.isSharedConnection() || !consumerInfo.isSharedChannel()) {
                if (consumerInfo.getChannel() != null && consumerInfo.getChannel().isOpen()) {
                    consumerInfo.getChannel().close();
                }
            }
            if (!consumerInfo.isSharedConnection()) {
                if (consumerInfo.getConnection() != null && consumerInfo.getConnection().isOpen()) {
                    consumerInfo.getConnection().close();
                }
            }
        } catch (IOException ex) {
            if (logger.isErrorEnabled()) {
                logger.error("cancel consumer[" + consumerInfo.getConsumerTag() + "] has failed, error: " + ex.getMessage());
            }
        } catch (TimeoutException ex) {
            if (logger.isErrorEnabled()) {
                logger.error("cancel consumer[" + consumerInfo.getConsumerTag() + "] is timeout, error: " + ex.getMessage());
            }
        }
    }
    
    public class ConsumerInfo {
        private final Connection connection;
        private final boolean sharedConnection;
        private final Channel channel;
        private final boolean sharedChannel;
        private final String queueName;
        private final boolean fixedQueue;
        private final Consumer consumer;
        private final String consumerTag;
        
        public ConsumerInfo(Connection connection, boolean sharedConnection, Channel channel, boolean sharedChannel, 
                String queueName, boolean fixedQueue, Consumer consumer, String consumerTag) {
            this.connection = connection;
            this.sharedConnection = sharedConnection;
            this.channel = channel;
            this.sharedChannel = sharedChannel;
            this.queueName = queueName;
            this.fixedQueue = fixedQueue;
            this.consumer = consumer;
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
        
        public Consumer getConsumer() {
            return consumer;
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
            if (logger.isInfoEnabled()) logger.info("[*] Cancel consumers, close channels, close connection.");
            
            if (producingChannel != null && producingChannel.isOpen()) producingChannel.close();
            if (producingConnection != null && producingConnection.isOpen()) producingConnection.close();
            
            if ("engine".equals(mode)) {
                for(ConsumerInfo consumerInfo: consumerInfos) {
                    this.cancelConsumer(consumerInfo);
                }
                consumerInfos.clear();
            }
            
            if (consumingChannel != null && consumingChannel.isOpen()) consumingChannel.close();
            if (consumingConnection != null && consumingConnection.isOpen()) consumingConnection.close();
        } catch (IOException exception) {
            if (logger.isErrorEnabled()) logger.error("close() has failed, exception: " + exception.getMessage());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (logger.isErrorEnabled()) logger.error("close() is timeout, exception: " + exception.getMessage());
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
            try {
                producingChannel = getProducingConnection().createChannel();
                producingChannel.addShutdownListener(new ShutdownListener() {
                    @Override
                    public void shutdownCompleted(ShutdownSignalException sse) {
                        if (logger.isInfoEnabled()) {
                            logger.info("Main channel[" + producingChannel.getChannelNumber() + "] has been shutdown");
                        }
                    }
                });
            } catch (IOException exception) {
                if (logger.isErrorEnabled()) logger.error("getChannel() has been failed, exception: " + exception.getMessage());
                throw exception;
            }
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
            if (logger.isTraceEnabled()) {
                logger.trace(MessageFormat.format("Exchange[{0}] binded to Queue[{1}] with key[{2}]", new Object[] {
                    _exchangeName, _queueName, _routingKey
                }));
            }
        }
    }
    
    private Pattern passwordPattern = Pattern.compile(":([^:]+)@");
    
    private String hidePasswordInUri(String uri) {
        return passwordPattern.matcher(uri).replaceAll(":******@");
    }
}