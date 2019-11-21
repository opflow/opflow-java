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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowConnectionException;
import com.devebot.opflow.exception.OpflowConsumerOverLimitException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowKeytool;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 *
 * @author drupalex
 */
public class OpflowEngine {

    public static final String[] PARAMETER_NAMES = new String[] {
        "uri", "host", "port", "virtualHost", "username", "password", "channelMax", "frameMax", "heartbeat",
        "threadPoolType", "threadPoolSize",
        "exchangeName", "exchangeType", "exchangeDurable", "routingKey", "otherKeys", "applicationId",
        "automaticRecoveryEnabled", "topologyRecoveryEnabled", "networkRecoveryInterval",
        "pkcs12File", "pkcs12Passphrase", "caCertFile", "serverCertFile", "trustStoreFile", "trustPassphrase"
    };

    private final static Logger LOG = LoggerFactory.getLogger(OpflowEngine.class);
    private final OpflowLogTracer logTracer;
    private final String engineId;

    private String mode;
    private ConnectionFactory factory;
    private Connection producingConnection;
    private Channel producingChannel;
    private Connection consumingConnection;
    private Channel consumingChannel;
    private List<ConsumerInfo> consumerInfos = new LinkedList<>();
    
    private String exchangeName;
    private String exchangeType;
    private Boolean exchangeDurable;
    private String routingKey;
    private String[] otherKeys;
    private String applicationId;

    private OpflowExporter exporter = OpflowExporter.getInstance();
    
    public OpflowEngine(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowUtil.ensureNotNull(params);
        
        engineId = OpflowUtil.getOptionField(params, "engineId", true);
        logTracer = OpflowLogTracer.ROOT.branch("engineId", engineId);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Engine[${engineId}].new()")
                .stringify());
        
        mode = params.containsKey("mode") ? params.get("mode").toString() : "engine";
        try {
            factory = new ConnectionFactory();
            
            String threadPoolType = null;
            if (params.get("threadPoolType") instanceof String) {
                threadPoolType = (String) params.get("threadPoolType");
            }
            
            Integer threadPoolSize = null;
            if (params.get("threadPoolSize") instanceof Integer) {
                threadPoolSize = (Integer)params.get("threadPoolSize");
            }
            if (threadPoolSize == null || threadPoolSize <= 0) {
                threadPoolSize = 2;
            }
            
            ExecutorService threadExecutor = null;
            if (null != threadPoolType) switch (threadPoolType) {
                case "cached":
                    threadExecutor = Executors.newCachedThreadPool();
                    break;
                case "fixed":
                    threadExecutor = Executors.newFixedThreadPool(threadPoolSize);
                    break;
                case "single":
                    threadExecutor = Executors.newSingleThreadExecutor();
                    break;
                case "single-scheduled":
                    threadExecutor = Executors.newSingleThreadScheduledExecutor();
                    break;
                case "scheduled":
                    threadExecutor = Executors.newScheduledThreadPool(threadPoolSize);
                    break;
                default:
                    break;
            }
            
            if (threadExecutor != null) {
                factory.setSharedExecutor(threadExecutor);
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .put("threadPoolType", threadPoolType)
                    .put("threadPoolSize", threadPoolSize)
                    .text("Engine[${engineId}] use SharedExecutor type: ${threadPoolType} / ${threadPoolSize}")
                    .stringify());
            } else {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .put("threadPoolType", threadPoolType)
                    .put("threadPoolSize", threadPoolSize)
                    .text("Engine[${engineId}] use default SharedExecutor")
                    .stringify());
            }
            
            String uri = (String) params.get("uri");
            if (uri != null && uri.length() > 0) {
                factory.setUri(uri);
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("uri", OpflowUtil.hidePasswordInUri(uri))
                        .text("Engine[${engineId}] make connection using URI: ${uri}")
                        .stringify());
            } else {
                String host = (String) params.getOrDefault("host", "localhost");
                factory.setHost(host);
                
                Integer port = null;
                if (params.get("port") instanceof Integer) {
                    factory.setPort(port = (Integer)params.get("port"));
                }
                
                String virtualHost = null;
                if (params.get("virtualHost") instanceof String) {
                    factory.setVirtualHost(virtualHost = (String) params.get("virtualHost"));
                }
                
                String username = null;
                if (params.get("username") instanceof String) {
                    factory.setUsername(username = (String) params.get("username"));
                }
                
                String password = null;
                if (params.get("password") instanceof String) {
                    factory.setPassword(password = (String) params.get("password"));
                }
                
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .put("host", host)
                    .put("port", port)
                    .put("virtualHost", virtualHost)
                    .put("username", username)
                    .put("password", OpflowUtil.maskPassword(password))
                    .text("Engine[${engineId}] make connection using parameters")
                    .stringify());
            }
            
            Integer channelMax = null;
            if (params.get("channelMax") instanceof Integer) {
                factory.setRequestedChannelMax(channelMax = (Integer)params.get("channelMax"));
            }

            Integer frameMax = null;
            if (params.get("frameMax") instanceof Integer) {
                factory.setRequestedFrameMax(frameMax = (Integer)params.get("frameMax"));
            }

            Integer heartbeat;
            if (params.get("heartbeat") instanceof Integer) {
                heartbeat = (Integer)params.get("heartbeat");
                if (heartbeat < 5) heartbeat = 5;
            } else {
                heartbeat = 20; // default 20 seconds
            }
            if (heartbeat != null) {
                factory.setRequestedHeartbeat(heartbeat);
            }

            Boolean automaticRecoveryEnabled = null;
            if (params.get("automaticRecoveryEnabled") instanceof Boolean) {
                factory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled = (Boolean)params.get("automaticRecoveryEnabled"));
            }

            Boolean topologyRecoveryEnabled = null;
            if (params.get("topologyRecoveryEnabled") instanceof Boolean) {
                factory.setTopologyRecoveryEnabled(topologyRecoveryEnabled = (Boolean)params.get("topologyRecoveryEnabled"));
            }

            Integer networkRecoveryInterval;
            if (params.get("networkRecoveryInterval") instanceof Integer) {
                networkRecoveryInterval = (Integer)params.get("networkRecoveryInterval");
                if (networkRecoveryInterval <= 0) networkRecoveryInterval = null;
            } else {
                networkRecoveryInterval = 2500; // change default from 5000 to 2500
            }
            if (networkRecoveryInterval != null) {
                factory.setNetworkRecoveryInterval(networkRecoveryInterval);
            }

            String pkcs12File = null;
            if (params.get("pkcs12File") instanceof String) {
                pkcs12File = (String) params.get("pkcs12File");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("pkcs12File", pkcs12File)
                        .text("Engine[${engineId}] - PKCS12 file: ${pkcs12File}")
                        .stringify());
            }

            String pkcs12Passphrase = null;
            if (params.get("pkcs12Passphrase") instanceof String) {
                pkcs12Passphrase = (String) params.get("pkcs12Passphrase");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("pkcs12Passphrase", OpflowUtil.maskPassword(pkcs12Passphrase))
                        .text("Engine[${engineId}] - PKCS12 passphrase: ${pkcs12Passphrase}")
                        .stringify());
            }

            String caCertFile = null;
            if (params.get("caCertFile") instanceof String) {
                caCertFile = (String) params.get("caCertFile");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("caCertFile", caCertFile)
                        .text("Engine[${engineId}] - CA file: ${caCertFile}")
                        .stringify());
            }

            String serverCertFile = null;
            if (params.get("serverCertFile") instanceof String) {
                serverCertFile = (String) params.get("serverCertFile");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("serverCertFile", serverCertFile)
                        .text("Engine[${engineId}] - server certificate file: ${serverCertFile}")
                        .stringify());
            }

            String trustStoreFile = null;
            if (params.get("trustStoreFile") instanceof String) {
                trustStoreFile = (String) params.get("trustStoreFile");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("trustStoreFile", trustStoreFile)
                        .text("Engine[${engineId}] - trust keystore file: ${trustStoreFile}")
                        .stringify());
            }

            String trustPassphrase = null;
            if (params.get("trustPassphrase") instanceof String) {
                trustPassphrase = (String) params.get("trustPassphrase");
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("trustPassphrase", OpflowUtil.maskPassword(trustPassphrase))
                        .text("Engine[${engineId}] - trust keystore passphrase: ${trustPassphrase}")
                        .stringify());
            }

            SSLContext sslContext = null;
            if (pkcs12File != null && pkcs12Passphrase != null) {
                if (caCertFile != null) {
                    sslContext = OpflowKeytool.buildSSLContextWithCertFile(pkcs12File, pkcs12Passphrase, caCertFile);
                } else if (serverCertFile != null) {
                    sslContext = OpflowKeytool.buildSSLContextWithCertFile(pkcs12File, pkcs12Passphrase, serverCertFile);
                } else if (trustStoreFile != null && trustPassphrase != null) {
                    sslContext = OpflowKeytool.buildSSLContextWithKeyStore(pkcs12File, pkcs12Passphrase,
                            trustStoreFile, trustPassphrase);
                }
            }

            if (sslContext != null) {
                factory.useSslProtocol(sslContext);
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .text("Engine[${engineId}] use SSL Protocol")
                    .stringify());
            } else {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .text("Engine[${engineId}] SSL context is empty")
                    .stringify());
            }

            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .put("channelMax", channelMax)
                    .put("frameMax", frameMax)
                    .put("heartbeat", heartbeat)
                    .put("automaticRecoveryEnabled", automaticRecoveryEnabled)
                    .put("topologyRecoveryEnabled", topologyRecoveryEnabled)
                    .put("networkRecoveryInterval", networkRecoveryInterval)
                    .text("Engine[${engineId}] make connection using parameters: "
                            + "channelMax: ${channelMax}, "
                            + "frameMax: ${frameMax}, "
                            + "heartbeat: ${heartbeat}, "
                            + "automaticRecoveryEnabled: ${automaticRecoveryEnabled}, "
                            + "topologyRecoveryEnabled: ${topologyRecoveryEnabled}, "
                            + "networkRecoveryInterval: ${networkRecoveryInterval}")
                    .stringify());

            this.assertConnection();
        } catch (IOException | URISyntaxException | KeyManagementException | NoSuchAlgorithmException | TimeoutException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] newConnection() has failed, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
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
            
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("exchangeName", exchangeName)
                        .put("exchangeType", exchangeType)
                        .put("exchangeDurable", exchangeDurable)
                        .put("routingKey", routingKey)
                        .put("otherKeys", otherKeys)
                        .put("applicationId", applicationId)
                        .text("Engine[${engineId}] exchangeName: '${exchangeName}' and routingKeys: ${routingKey}")
                        .stringify());
        } catch (IOException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] exchangeDeclare has failed, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
            throw new OpflowBootstrapException("exchangeDeclare has failed", exception);
        } catch (TimeoutException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] exchangeDeclare is timeout, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
            throw new OpflowBootstrapException("it maybe too slow or unstable network", exception);
        }
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Engine[${engineId}].new() end!")
                .stringify());
        
        exporter.changeComponentInstance(OpflowExporter.GaugeAction.INC, "engine", engineId);
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
            
            headers.put("publishedTime", OpflowUtil.getCurrentTimeString());
            
            String requestId = OpflowUtil.getRequestId(headers, false);
            if (requestId == null) {
                headers.put("requestId", requestId = OpflowUtil.getLogID());
            }
            propBuilder.headers(headers);
            
            if (OpflowLogTracer.has(LOG, "info")) logProduce = logTracer.branch("requestId", requestId);
            
            if (OpflowLogTracer.has(LOG, "info") && logProduce != null) LOG.info(logProduce
                    .put("appId", appId)
                    .put("customKey", customKey)
                    .text("Request[${requestId}] - Engine[${engineId}] - produce() is invoked")
                    .stringify());
            
            Channel _channel = getProducingChannel();
            if (_channel == null || !_channel.isOpen()) {
                throw new OpflowOperationException("Channel is null or has been closed");
            }
            _channel.basicPublish(this.exchangeName, customKey, propBuilder.build(), body);
        } catch (IOException exception) {
            if (OpflowLogTracer.has(LOG, "error") && logProduce != null) LOG.error(logProduce
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Request[${requestId}] - produce() has failed")
                    .stringify());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (OpflowLogTracer.has(LOG, "error") && logProduce != null) LOG.error(logProduce
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Request[${requestId}] - produce() is timeout")
                    .stringify());
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowListener listener, final Map<String, Object> options) {
        final Map<String, Object> opts = OpflowUtil.ensureNotNull(options);
        final String _consumerId = OpflowUtil.getOptionField(opts, "consumerId", true);
        final OpflowLogTracer logConsume = logTracer.branch("consumerId", _consumerId);
        
        if (OpflowLogTracer.has(LOG, "info")) LOG.info(logConsume
                .text("Consumer[${consumerId}].consume() is invoked in Engine[${engineId}]")
                .stringify());
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
            final boolean opts_durable = !Boolean.FALSE.equals(opts.get("durable"));
            final boolean opts_exclusive = Boolean.TRUE.equals(opts.get("exclusive"));
            final boolean opts_autoDelete = Boolean.TRUE.equals(opts.get("autoDelete"));
            AMQP.Queue.DeclareOk _declareOk;
            if (opts_queueName != null) {
                _declareOk = _channel.queueDeclare(opts_queueName, opts_durable, opts_exclusive, opts_autoDelete, null);
                _fixedQueue = true;
            } else {
                _declareOk = _channel.queueDeclare();
                _fixedQueue = false;
            }
            _queueName = _declareOk.getQueue();
            final Integer _consumerLimit = (Integer) opts.get("consumerLimit");
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logConsume
                    .put("consumerCount", _declareOk.getConsumerCount())
                    .put("consumerLimit", _consumerLimit)
                    .text("Consumer[${consumerId}].consume() - consumerCount(${consumerCount})/consumerLimit(${consumerLimit})")
                    .stringify());
            if (_consumerLimit != null && _consumerLimit > 0) {
                if (_declareOk.getConsumerCount() >= _consumerLimit) {
                    if (OpflowLogTracer.has(LOG, "error")) LOG.error(logConsume
                            .put("consumerCount", _declareOk.getConsumerCount())
                            .put("consumerLimit", _consumerLimit)
                            .text("Consumer[${consumerId}].consume() - consumerCount exceed limit")
                            .stringify());
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
            
            final Boolean _requeueFailure;
            if (opts.get("requeueFailure") != null && opts.get("requeueFailure") instanceof Boolean) {
                _requeueFailure = (Boolean) opts.get("requeueFailure");
            } else {
                _requeueFailure = Boolean.FALSE;
            }
            
            final Consumer _consumer = new DefaultConsumer(_channel) {
                private void invokeAck(Envelope envelope, boolean success) throws IOException {
                    if (!_autoAck) {
                        if (success) {
                            _channel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            if (!_requeueFailure) {
                                _channel.basicAck(envelope.getDeliveryTag(), false);
                            } else {
                                _channel.basicNack(envelope.getDeliveryTag(), false, true);
                            }
                        }
                    }
                }
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body) throws IOException {
                    final String requestID = OpflowUtil.getRequestId(properties.getHeaders(), false);
                    
                    final OpflowLogTracer logRequest = logConsume.branch("requestId", requestID);
                    
                    if (OpflowLogTracer.has(LOG, "info")) LOG.info(logRequest
                            .put("appId", properties.getAppId())
                            .put("deliveryTag", envelope.getDeliveryTag())
                            .put("consumerTag", consumerTag)
                            .text("Request[${requestId}] - Consumer[${consumerId}] - consumer received a message")
                            .stringify());
                    
                    if (OpflowLogTracer.has(LOG, "trace")) {
                        if (body.length <= 4096) {
                            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logRequest
                                    .put("bodyHead", new String(body, "UTF-8"))
                                    .put("bodyLength", body.length)
                                    .text("Request[${requestId}] body head (4096 bytes)")
                                    .stringify());
                        } else {
                            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logRequest
                                    .put("bodyLength", body.length)
                                    .text("Request[${requestId}] body size too large (>4KB)")
                                    .stringify());
                        }
                    }
                    
                    try {
                        if (applicationId == null || applicationId.equals(properties.getAppId())) {
                            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logRequest
                                    .text("Request[${requestId}] invoke listener.processMessage()")
                                    .stringify());
                            
                            boolean captured = listener.processMessage(body, properties, _replyToName, _channel, consumerTag);
                            
                            if (captured) {
                                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logRequest
                                        .text("Request[${requestId}] has finished successfully")
                                        .stringify());
                            } else {
                                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logRequest
                                        .text("Request[${requestId}] has not matched the criteria, skipped")
                                        .stringify());
                            }
                            
                            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logRequest
                                    .put("deliveryTag", envelope.getDeliveryTag())
                                    .put("consumerTag", consumerTag)
                                    .text("Request[${requestId}] invoke ACK")
                                    .stringify());
                            
                            invokeAck(envelope, true);
                        } else {
                            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logRequest
                                    .put("applicationId", applicationId)
                                    .text("Request[${requestId}] has been rejected, mismatched applicationId")
                                    .stringify());
                            invokeAck(envelope, false);
                        }
                    } catch (IOException ex) {
                        // catch ALL of Error here: don't let it harm our service/close the channel
                        if (OpflowLogTracer.has(LOG, "error")) LOG.error(logRequest
                                .put("deliveryTag", envelope.getDeliveryTag())
                                .put("consumerTag", consumerTag)
                                .put("exceptionClass", ex.getClass().getName())
                                .put("exceptionMessage", ex.getMessage())
                                .put("autoAck", _autoAck)
                                .put("requeueFailure", _requeueFailure)
                                .text("Request[${requestId}] has been failed. Service still alive")
                                .stringify());
                        invokeAck(envelope, false);
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (OpflowLogTracer.has(LOG, "info")) LOG.info(logConsume
                            .put("consumerTag", consumerTag)
                            .text("Consumer[${consumerId}].consume() - handle CancelOk event")
                            .stringify());
                }
                
                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (OpflowLogTracer.has(LOG, "info")) LOG.info(logConsume
                            .put("consumerTag", consumerTag)
                            .text("Consumer[${consumerId}].consume() - handle ShutdownSignal event")
                            .stringify());
                }
            };
            
            final String _consumerTag = _channel.basicConsume(_queueName, _autoAck, _consumer);
            
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logConsume
                    .put("queueName", _queueName)
                    .put("consumerTag", _consumerTag)
                    .put("channelNumber", _channel.getChannelNumber())
                    .text("Consumer[${consumerId}].consume() create consumer[${consumerTag}]/queue[${queueName}]")
                    .stringify());
            ConsumerInfo info = new ConsumerInfo(_connection, !_forceNewConnection, 
                    _channel, !_forceNewChannel, _queueName, _fixedQueue, _consumerId, _consumerTag);
            if ("engine".equals(mode)) consumerInfos.add(info);
            return info;
        } catch(IOException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logConsume
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Consumer[${consumerId}].consume() - has failed")
                    .stringify());
            throw new OpflowOperationException(exception);
        } catch(TimeoutException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logConsume
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Consumer[${consumerId}].consume() - is timeout")
                    .stringify());
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
            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logCancel
                    .put("queueName", consumerInfo.getQueueName())
                    .text("Consumer[${consumerId}].cancelConsumer() - consumer will be cancelled")
                    .stringify());

            consumerInfo.getChannel().basicCancel(consumerInfo.getConsumerTag());

            if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logCancel
                    .text("Consumer[${consumerId}].cancelConsumer() - consumer has been cancelled")
                    .stringify());

            if (!consumerInfo.isSharedConnection() || !consumerInfo.isSharedChannel()) {
                if (consumerInfo.getChannel() != null && consumerInfo.getChannel().isOpen()) {
                    if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logCancel
                            .tags("sharedConsumingChannelClosed")
                            .text("Consumer[${consumerId}] shared consumingChannel is closing")
                            .stringify());
                    consumerInfo.getChannel().close();
                }
            }

            if (!consumerInfo.isSharedConnection()) {
                if (consumerInfo.getConnection() != null && consumerInfo.getConnection().isOpen()) {
                    if (OpflowLogTracer.has(LOG, "debug")) LOG.debug(logCancel
                            .tags("sharedConsumingConnectionClosed")
                            .text("Consumer[${consumerId}] shared consumingConnection is closing")
                            .stringify());
                    consumerInfo.getConnection().close();
                }
            }
        } catch (IOException ex) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logCancel
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionMessage", ex.getMessage())
                    .text("Consumer[${consumerId}].cancelConsumer() - has failed")
                    .stringify());
        } catch (TimeoutException ex) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logCancel
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionMessage", ex.getMessage())
                    .text("Consumer[${consumerId}].cancelConsumer() - is timeout")
                    .stringify());
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
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Engine[${engineId}].close() - close producingChannel, producingConnection")
                .stringify());
            if (producingChannel != null && producingChannel.isOpen()) {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .tags("sharedProducingChannelClosed")
                        .text("Engine[${engineId}].close() shared producingChannel is closing")
                        .stringify());
                producingChannel.close();
            }
            if (producingConnection != null && producingConnection.isOpen()) {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .tags("sharedProducingConnectionClosed")
                        .text("Engine[${engineId}].close() shared producingConnection is closing")
                        .stringify());
                producingConnection.close();
            }
            
            if ("engine".equals(mode)) {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .put("mode", mode)
                        .text("Engine[${engineId}].close() - cancel consumers")
                        .stringify());
                for(ConsumerInfo consumerInfo: consumerInfos) {
                    this.cancelConsumer(consumerInfo);
                }
                consumerInfos.clear();
            }
            
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                .text("Engine[${engineId}].close() - close consumingChannel, consumingConnection")
                .stringify());
            if (consumingChannel != null && consumingChannel.isOpen()) {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .tags("sharedConsumingChannelClosed")
                        .text("Engine[${engineId}].close() shared consumingChannel is closing")
                        .stringify());
                consumingChannel.close();
            }
            if (consumingConnection != null && consumingConnection.isOpen()) {
                if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                        .tags("sharedConsumingConnectionClosed")
                        .text("Engine[${engineId}].close() shared consumingConnection is closing")
                        .stringify());
                consumingConnection.close();
            }

        } catch (IOException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                    .text("Engine[${engineId}].close() has failed")
                    .stringify());
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (OpflowLogTracer.has(LOG, "error")) LOG.error(logTracer
                    .text("Engine[${engineId}].close() is timeout")
                    .stringify());
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
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("sharedProducingConnectionCreated")
                    .text("Engine[${engineId}] shared producingConnection is created")
                    .stringify());
            producingConnection = factory.newConnection();
            exporter.incEngineConnectionGauge(factory, "producing");
        }
        return producingConnection;
    }
    
    private Channel getProducingChannel() throws IOException, TimeoutException {
        if (producingChannel == null || !producingChannel.isOpen()) {
            producingChannel = getProducingConnection().createChannel();
            producingChannel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                            .put("channelNumber", producingChannel.getChannelNumber())
                            .text("Engine[${engineId}] producingChannel has been shutdown")
                            .stringify());
                }
            });
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("sharedProducingChannelCreated")
                    .text("Engine[${engineId}] shared producingChannel is created")
                    .stringify());
        }
        return producingChannel;
    }
    
    private Connection getConsumingConnection(boolean forceNewConnection) throws IOException, TimeoutException {
        if (forceNewConnection) {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("privateConsumingConnectionCreated")
                    .text("Engine[${engineId}] private consumingConnection is created")
                    .stringify());
            return factory.newConnection();
        }
        if (consumingConnection == null || !consumingConnection.isOpen()) {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("sharedConsumingConnectionCreated")
                    .text("Engine[${engineId}] shared consumingConnection is created")
                    .stringify());
            consumingConnection = factory.newConnection();
            exporter.incEngineConnectionGauge(factory, "consuming");
        }
        return consumingConnection;
    }
    
    private Channel getConsumingChannel(boolean forceNewConnection, boolean forceNewChannel) throws IOException, TimeoutException {
        if (forceNewConnection) {
            return getConsumingConnection(forceNewConnection).createChannel();
        }
        if (forceNewChannel) {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("privateConsumingChannelCreated")
                    .text("Engine[${engineId}] private consumingChannel is created")
                    .stringify());
            return getConsumingConnection(false).createChannel();
        }
        if (consumingChannel == null || !consumingChannel.isOpen()) {
            consumingChannel = getConsumingConnection(false).createChannel();
            consumingChannel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException sse) {
                    if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                            .put("channelNumber", consumingChannel.getChannelNumber())
                            .text("Engine[${engineId}] consumingChannel has been shutdown")
                            .stringify());
                }
            });
            if (OpflowLogTracer.has(LOG, "info")) LOG.info(logTracer
                    .tags("sharedConsumingChannelCreated")
                    .text("Engine[${engineId}] shared consumingChannel is created")
                    .stringify());
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
            if (OpflowLogTracer.has(LOG, "trace")) LOG.trace(logTracer
                    .put("exchangeName", _exchangeName)
                    .put("queueName", _queueName)
                    .put("routingKey", _routingKey)
                    .text("Binds Exchange to Queue")
                    .stringify());
        }
    }
    
    protected void finalize() throws Throwable {
        try {
            close();
        } finally {
            exporter.changeComponentInstance(OpflowExporter.GaugeAction.DEC, "engine", engineId);
        }
    }
}