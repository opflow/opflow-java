package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowConnectionException;
import com.devebot.opflow.exception.OpflowConsumerOverLimitException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowCollectionUtil;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowKeytool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowSysInfo;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author drupalex
 */
public class OpflowEngine implements AutoCloseable {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    public static final String[] PARAMETER_NAMES = new String[] {
        OpflowConstant.AMQP_CONARG_URI,
        OpflowConstant.AMQP_CONARG_HOST,
        OpflowConstant.AMQP_CONARG_PORT,
        OpflowConstant.AMQP_CONARG_VHOST,
        OpflowConstant.AMQP_CONARG_USERNAME,
        OpflowConstant.AMQP_CONARG_PASSWORD,
        OpflowConstant.AMQP_CONARG_REQUESTED_CHANNEL_MAX,
        OpflowConstant.AMQP_CONARG_REQUESTED_FRAME_MAX,
        OpflowConstant.AMQP_CONARG_REQUESTED_HEARTBEAT,
        OpflowConstant.AMQP_CONARG_AUTOMATIC_RECOVERY_ENABLED,
        OpflowConstant.AMQP_CONARG_TOPOLOGY_RECOVERY_ENABLED,
        OpflowConstant.AMQP_CONARG_NETWORK_RECOVERY_INTERVAL,
        OpflowConstant.AMQP_CONARG_PKCS12_FILE,
        OpflowConstant.AMQP_CONARG_PKCS12_PASSPHRASE,
        OpflowConstant.AMQP_CONARG_CA_CERT_FILE,
        OpflowConstant.AMQP_CONARG_SERVER_CERT_FILE,
        OpflowConstant.AMQP_CONARG_TRUST_STORE_FILE,
        OpflowConstant.AMQP_CONARG_TRUST_PASSPHRASE,
        OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_TYPE,
        OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_SIZE,
        OpflowConstant.OPFLOW_COMMON_APP_ID,
    };
    
    public static final String[] SHARED_PARAMETERS = OpflowCollectionUtil.mergeArrays(PARAMETER_NAMES, new String[] {
        OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME,
        OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_TYPE,
        OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE,
        OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY,
        OpflowConstant.OPFLOW_CONSUMING_BINDING_KEYS,
    });
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowEngine.class);
    private final OpflowLogTracer logTracer;
    private final String componentId;
    private final OpflowPromMeasurer measurer;
    
    private String owner;
    private ConnectionFactory factory;
    private String producingConnectionId;
    private volatile Connection producingConnection;
    private volatile Channel producingChannel;
    private volatile BlockedListener producingBlockedListener;
    private String consumingConnectionId;
    private volatile Connection consumingConnection;
    private volatile Channel consumingChannel;
    private volatile BlockedListener consumingBlockedListener;
    private List<ConsumerInfo> consumerInfos = new LinkedList<>();
    
    private ExecutorService sharedExecutor;
    private String threadPoolType = null;
    private Integer threadPoolSize = null;
    
    private final Object producingConnectionLock = new Object();
    private final Object producingChannelLock = new Object();
    private final Object producingBlockedListenerLock = new Object();
    private final Object consumingConnectionLock = new Object();
    private final Object consumingChannelLock = new Object();
    private final Object consumingBlockedListenerLock = new Object();
    
    private String exchangeName;
    private String exchangeType;
    private Boolean exchangeDurable;
    private String routingKey;
    private String applicationId;
    
    public OpflowEngine(Map<String, Object> params) throws OpflowBootstrapException {
        params = OpflowObjectTree.ensureNonNull(params);
        
        componentId = OpflowUtil.getOptionField(params, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(params, OpflowConstant.COMP_MEASURER, OpflowPromMeasurer.NULL);
        
        logTracer = OpflowLogTracer.ROOT.branch("engineId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Engine[${engineId}][${instanceId}].new()")
                .stringify());
        
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .put("protoVersion", CONST.AMQP_PROTOCOL_VERSION)
                .put("headers", OpflowJsonTool.toString(new String[] {
                        CONST.AMQP_HEADER_ROUTINE_ID,
                        CONST.AMQP_HEADER_ROUTINE_TIMESTAMP,
                        CONST.AMQP_HEADER_ROUTINE_SCOPE,
                        CONST.AMQP_HEADER_ROUTINE_SIGNATURE,
                        CONST.AMQP_HEADER_ROUTINE_TAGS
                }))
                .text("Engine[${engineId}][${instanceId}] - apply the protocol version [${protoVersion}] with AMQP headers: [${headers}]")
                .stringify());
        
        owner = OpflowConstant.COMP_ENGINE;
        if (params.containsKey(OpflowConstant.OPFLOW_COMMON_INSTANCE_OWNER)) {
            owner = params.get(OpflowConstant.OPFLOW_COMMON_INSTANCE_OWNER).toString();
        }
        
        try {
            factory = new ConnectionFactory();
            
            String uri = (String) params.get(OpflowConstant.AMQP_CONARG_URI);
            if (uri != null && uri.length() > 0) {
                factory.setUri(uri);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("uri", OpflowUtil.hidePasswordInUri(uri))
                        .text("Engine[${engineId}] make connection using URI: ${uri}")
                        .stringify());
            } else {
                String host = (String) params.getOrDefault(OpflowConstant.AMQP_CONARG_HOST, "localhost");
                factory.setHost(host);
                
                Integer port = null;
                if (params.get(OpflowConstant.AMQP_CONARG_PORT) instanceof Integer) {
                    factory.setPort(port = (Integer)params.get(OpflowConstant.AMQP_CONARG_PORT));
                }
                
                String virtualHost = null;
                if (params.get(OpflowConstant.AMQP_CONARG_VHOST) instanceof String) {
                    factory.setVirtualHost(virtualHost = (String) params.get(OpflowConstant.AMQP_CONARG_VHOST));
                }
                
                String username = null;
                if (params.get(OpflowConstant.AMQP_CONARG_USERNAME) instanceof String) {
                    factory.setUsername(username = (String) params.get(OpflowConstant.AMQP_CONARG_USERNAME));
                }
                
                String password = null;
                if (params.get(OpflowConstant.AMQP_CONARG_PASSWORD) instanceof String) {
                    factory.setPassword(password = (String) params.get(OpflowConstant.AMQP_CONARG_PASSWORD));
                }
                
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("host", host)
                        .put("port", port)
                        .put("virtualHost", virtualHost)
                        .put("username", username)
                        .put("password", OpflowUtil.maskPassword(password))
                        .text("Engine[${engineId}] make connection using parameters")
                        .stringify());
            }
            
            Integer channelMax = null;
            if (params.get(OpflowConstant.AMQP_CONARG_REQUESTED_CHANNEL_MAX) instanceof Integer) {
                factory.setRequestedChannelMax(channelMax = (Integer)params.get(OpflowConstant.AMQP_CONARG_REQUESTED_CHANNEL_MAX));
            }

            Integer frameMax = null;
            if (params.get(OpflowConstant.AMQP_CONARG_REQUESTED_FRAME_MAX) instanceof Integer) {
                factory.setRequestedFrameMax(frameMax = (Integer)params.get(OpflowConstant.AMQP_CONARG_REQUESTED_FRAME_MAX));
            }

            Integer heartbeat;
            if (params.get(OpflowConstant.AMQP_CONARG_REQUESTED_HEARTBEAT) instanceof Integer) {
                heartbeat = (Integer)params.get(OpflowConstant.AMQP_CONARG_REQUESTED_HEARTBEAT);
                if (heartbeat < 5) heartbeat = 5;
            } else {
                heartbeat = 20; // default 20 seconds
            }
            if (heartbeat != null) {
                factory.setRequestedHeartbeat(heartbeat);
            }

            Boolean automaticRecoveryEnabled = null;
            if (params.get(OpflowConstant.AMQP_CONARG_AUTOMATIC_RECOVERY_ENABLED) instanceof Boolean) {
                factory.setAutomaticRecoveryEnabled(automaticRecoveryEnabled = (Boolean)params.get(OpflowConstant.AMQP_CONARG_AUTOMATIC_RECOVERY_ENABLED));
            }

            Boolean topologyRecoveryEnabled = null;
            if (params.get(OpflowConstant.AMQP_CONARG_TOPOLOGY_RECOVERY_ENABLED) instanceof Boolean) {
                factory.setTopologyRecoveryEnabled(topologyRecoveryEnabled = (Boolean)params.get(OpflowConstant.AMQP_CONARG_TOPOLOGY_RECOVERY_ENABLED));
            }

            Integer networkRecoveryInterval;
            if (params.get(OpflowConstant.AMQP_CONARG_NETWORK_RECOVERY_INTERVAL) instanceof Integer) {
                networkRecoveryInterval = (Integer)params.get(OpflowConstant.AMQP_CONARG_NETWORK_RECOVERY_INTERVAL);
                if (networkRecoveryInterval <= 0) networkRecoveryInterval = null;
            } else {
                networkRecoveryInterval = 2500; // change default from 5000 to 2500
            }
            if (networkRecoveryInterval != null) {
                factory.setNetworkRecoveryInterval(networkRecoveryInterval);
            }

            String pkcs12File = null;
            if (params.get(OpflowConstant.AMQP_CONARG_PKCS12_FILE) instanceof String) {
                pkcs12File = (String) params.get(OpflowConstant.AMQP_CONARG_PKCS12_FILE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("pkcs12File", pkcs12File)
                        .text("Engine[${engineId}] - PKCS12 file: ${pkcs12File}")
                        .stringify());
            }

            String pkcs12Passphrase = null;
            if (params.get(OpflowConstant.AMQP_CONARG_PKCS12_PASSPHRASE) instanceof String) {
                pkcs12Passphrase = (String) params.get(OpflowConstant.AMQP_CONARG_PKCS12_PASSPHRASE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("pkcs12Passphrase", OpflowUtil.maskPassword(pkcs12Passphrase))
                        .text("Engine[${engineId}] - PKCS12 passphrase: ${pkcs12Passphrase}")
                        .stringify());
            }

            String caCertFile = null;
            if (params.get(OpflowConstant.AMQP_CONARG_CA_CERT_FILE) instanceof String) {
                caCertFile = (String) params.get(OpflowConstant.AMQP_CONARG_CA_CERT_FILE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("caCertFile", caCertFile)
                        .text("Engine[${engineId}] - CA file: ${caCertFile}")
                        .stringify());
            }

            String serverCertFile = null;
            if (params.get(OpflowConstant.AMQP_CONARG_SERVER_CERT_FILE) instanceof String) {
                serverCertFile = (String) params.get(OpflowConstant.AMQP_CONARG_SERVER_CERT_FILE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("serverCertFile", serverCertFile)
                        .text("Engine[${engineId}] - server certificate file: ${serverCertFile}")
                        .stringify());
            }

            String trustStoreFile = null;
            if (params.get(OpflowConstant.AMQP_CONARG_TRUST_STORE_FILE) instanceof String) {
                trustStoreFile = (String) params.get(OpflowConstant.AMQP_CONARG_TRUST_STORE_FILE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("trustStoreFile", trustStoreFile)
                        .text("Engine[${engineId}] - trust keystore file: ${trustStoreFile}")
                        .stringify());
            }

            String trustPassphrase = null;
            if (params.get(OpflowConstant.AMQP_CONARG_TRUST_PASSPHRASE) instanceof String) {
                trustPassphrase = (String) params.get(OpflowConstant.AMQP_CONARG_TRUST_PASSPHRASE);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
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
                    sslContext = OpflowKeytool.buildSSLContextWithKeyStore(pkcs12File, pkcs12Passphrase, trustStoreFile, trustPassphrase);
                }
            }

            if (sslContext != null) {
                factory.useSslProtocol(sslContext);
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .text("Engine[${engineId}] use SSL Protocol")
                        .stringify());
            } else {
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .text("Engine[${engineId}] SSL context is empty")
                        .stringify());
            }

            threadPoolType = null;
            threadPoolSize = null;
            if (params.get(OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_TYPE) instanceof String) {
                threadPoolType = (String) params.get(OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_TYPE);
                if (params.get(OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_SIZE) instanceof Integer) {
                    threadPoolSize = (Integer)params.get(OpflowConstant.AMQP_CONARG_SHARED_THREAD_POOL_SIZE);
                }
                if (threadPoolSize == null || threadPoolSize <= 0) {
                    threadPoolSize = OpflowSysInfo.getNumberOfProcessors();
                }
                if (threadPoolSize <= 0) {
                    threadPoolSize = 2;
                }
            }
            
            if (threadPoolType != null) {
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("threadPoolType", threadPoolType)
                        .put("threadPoolSize", threadPoolSize)
                        .text("Engine[${engineId}] use SharedExecutor type: ${threadPoolType} / ${threadPoolSize}")
                        .stringify());
            } else {
                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .put("threadPoolType", threadPoolType)
                        .put("threadPoolSize", threadPoolSize)
                        .text("Engine[${engineId}] use default SharedExecutor")
                        .stringify());
            }
            
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
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
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] newConnection() has failed, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
            throw new OpflowConnectionException("connection refused, invalid connection parameters", exception);
        }
        
        try {
            if (params.get(OpflowConstant.OPFLOW_COMMON_APP_ID) instanceof String) {
                applicationId = (String) params.get(OpflowConstant.OPFLOW_COMMON_APP_ID);
            }
            
            if (params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME) instanceof String) {
                exchangeName = (String) params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME);
            }
            
            if (params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_TYPE) instanceof String) {
                exchangeType = (String) params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_TYPE);
            }
            if (exchangeType == null) exchangeType = "direct";
            
            if (params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE) instanceof Boolean) {
                exchangeDurable = (Boolean) params.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_DURABLE);
            }
            if (exchangeDurable == null) exchangeDurable = true;
            
            if (exchangeName != null) {
                getProducingChannel().exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
            }
            
            if (params.get(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY) instanceof String) {
                routingKey = (String) params.get(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY);
            }
            
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .put("exchangeName", exchangeName)
                    .put("exchangeType", exchangeType)
                    .put("exchangeDurable", exchangeDurable)
                    .put("routingKey", routingKey)
                    .put("applicationId", applicationId)
                    .text("Engine[${engineId}] exchangeName: '${exchangeName}' and routingKeys: ${routingKey}")
                    .stringify());
        } catch (IOException exception) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] exchangeDeclare has failed, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
            throw new OpflowBootstrapException("exchangeDeclare has failed", exception);
        } catch (TimeoutException exception) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Engine[${engineId}] exchangeDeclare is timeout, exception[${exceptionClass}]: ${exceptionMessage}")
                    .stringify());
            throw new OpflowBootstrapException("it maybe too slow or unstable network", exception);
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Engine[${engineId}][${instanceId}].new() end!")
                .stringify());
        
        measurer.updateComponentInstance(OpflowConstant.COMP_ENGINE, componentId, OpflowPromMeasurer.GaugeAction.INC);
    }

    public String getExchangeName() {
        return exchangeName;
    }
    
    public String getExchangeType() {
        return exchangeType;
    }

    public Boolean getExchangeDurable() {
        return exchangeDurable;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public String getApplicationId() {
        return applicationId;
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers) {
        produce(body, headers, null, null, null);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, AMQP.BasicProperties.Builder propBuilder) {
        produce(body, headers, propBuilder, null, null);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, Map<String, Object> override) {
        produce(body, headers, null, override, null);
    }
    
    public void produce(final byte[] body, final Map<String, Object> headers, AMQP.BasicProperties.Builder propBuilder, Map<String, Object> override, OpflowLogTracer reqTracer) {
        propBuilder = (propBuilder == null) ? new AMQP.BasicProperties.Builder() : propBuilder;
        
        try {
            String appId = this.applicationId;
            String reqExchangeName = this.exchangeName;
            String reqRoutingKey = this.routingKey;
            
            if (override != null) {
                if (override.get(OpflowConstant.OPFLOW_COMMON_APP_ID) instanceof String) {
                    appId = (String) override.get(OpflowConstant.OPFLOW_COMMON_APP_ID);
                }
                
                if (override.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME) instanceof String) {
                    reqExchangeName = (String) override.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME);
                }
                
                if (override.get(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY) instanceof String) {
                    reqRoutingKey = (String) override.get(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY);
                }
                
                if (override.get(OpflowConstant.OPFLOW_CONSUMING_REPLY_TO) instanceof String) {
                    propBuilder.replyTo(override.get(OpflowConstant.OPFLOW_CONSUMING_REPLY_TO).toString());
                }
            }
            
            propBuilder.appId(appId);
            propBuilder.headers(headers);
            
            if (reqTracer == null && logTracer.ready(LOG, Level.INFO)) {
                String routineId = OpflowUtil.getRoutineId(headers);
                String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
                reqTracer = logTracer.branch(CONST.REQUEST_TIME, routineTimestamp)
                        .branch(CONST.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(headers));
            }
            
            if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) {
                LOG.info(reqTracer
                        .put("engineId", componentId)
                        .put("appId", appId)
                        .put("routingKey", reqRoutingKey)
                        .text("Request[${requestId}][${requestTime}][x-engine-msg-publish] - Engine[${engineId}][${instanceId}] - produce() is invoked")
                        .stringify());
            }
            
            Channel _channel = getProducingChannel();
            if (_channel == null || !_channel.isOpen()) {
                throw new OpflowOperationException("Channel is null or has been closed");
            }
            _channel.basicPublish(reqExchangeName, reqRoutingKey, propBuilder.build(), body);
        } catch (IOException exception) {
            if (reqTracer != null && reqTracer.ready(LOG, Level.ERROR)) {
                LOG.error(reqTracer
                        .put("exceptionClass", exception.getClass().getName())
                        .put("exceptionMessage", exception.getMessage())
                        .text("Request[${requestId}][${requestTime}][x-engine-msg-publish-failed] - produce() has failed")
                        .stringify());
            }
            throw new OpflowOperationException(exception);
        } catch (TimeoutException exception) {
            if (reqTracer != null && reqTracer.ready(LOG, Level.ERROR)) {
                LOG.error(reqTracer
                        .put("exceptionClass", exception.getClass().getName())
                        .put("exceptionMessage", exception.getMessage())
                        .text("Request[${requestId}][${requestTime}][x-engine-msg-publish-timeout] - produce() is timeout")
                        .stringify());
            }
            throw new OpflowOperationException(exception);
        }
    }
    
    public ConsumerInfo consume(final OpflowEngine.Listener listener, final Map<String, Object> options) {
        final Map<String, Object> opts = OpflowObjectTree.ensureNonNull(options);
        final String _consumerId = OpflowUtil.getOptionField(opts, OpflowConstant.OPFLOW_CONSUMING_CONSUMER_ID, true);
        final OpflowLogTracer logConsume = logTracer.branch("consumerId", _consumerId);
        
        if (logConsume.ready(LOG, Level.INFO)) LOG.info(logConsume
                .text("Consumer[${consumerId}].consume() is invoked in Engine[${engineId}]")
                .stringify());
        try {
            final boolean _forceNewConnection = Boolean.TRUE.equals(opts.get("forceNewConnection"));
            final boolean _forceNewChannel = Boolean.TRUE.equals(opts.get("forceNewChannel"));
            final Channel _channel = getConsumingChannel(_forceNewConnection, _forceNewChannel);
            final Connection _connection = _channel.getConnection();
            
            Integer _prefetchCount = null;
            if (opts.get(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT) instanceof Integer) {
                _prefetchCount = (Integer) opts.get(OpflowConstant.OPFLOW_CONSUMING_PREFETCH_COUNT);
            }
            if (_prefetchCount != null && _prefetchCount > 0) {
                _channel.basicQos(_prefetchCount);
            }
            
            final String _queueName;
            final boolean _fixedQueue;
            String opts_queueName = (String) opts.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_NAME);
            final boolean opts_durable = !Boolean.FALSE.equals(opts.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_DURABLE));
            final boolean opts_exclusive = Boolean.TRUE.equals(opts.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_EXCLUSIVE));
            final boolean opts_autoDelete = Boolean.TRUE.equals(opts.get(OpflowConstant.OPFLOW_CONSUMING_QUEUE_AUTO_DELETE));
            AMQP.Queue.DeclareOk _declareOk;
            if (opts_queueName != null) {
                _declareOk = _channel.queueDeclare(opts_queueName, opts_durable, opts_exclusive, opts_autoDelete, null);
                _fixedQueue = true;
            } else {
                _declareOk = _channel.queueDeclare();
                _fixedQueue = false;
            }
            _queueName = _declareOk.getQueue();
            final Integer _consumerLimit = (Integer) opts.get(OpflowConstant.OPFLOW_CONSUMING_CONSUMER_LIMIT);
            if (logConsume.ready(LOG, Level.TRACE)) LOG.trace(logConsume
                    .put("consumerCount", _declareOk.getConsumerCount())
                    .put("consumerLimit", _consumerLimit)
                    .text("Consumer[${consumerId}].consume() - consumerCount(${consumerCount})/consumerLimit(${consumerLimit})")
                    .stringify());
            if (_consumerLimit != null && _consumerLimit > 0) {
                if (_declareOk.getConsumerCount() >= _consumerLimit) {
                    if (logConsume.ready(LOG, Level.ERROR)) LOG.error(logConsume
                            .put("consumerCount", _declareOk.getConsumerCount())
                            .put("consumerLimit", _consumerLimit)
                            .text("Consumer[${consumerId}].consume() - consumerCount exceed limit")
                            .stringify());
                    String errorMessage = "consumerLimit exceed: " + _declareOk.getConsumerCount() + "/" + _consumerLimit;
                    throw new OpflowConsumerOverLimitException(errorMessage);
                }
            }
            
            final String _exchangeName = (String) opts.get(OpflowConstant.OPFLOW_PRODUCING_EXCHANGE_NAME);
            final String _routingKey = (String) opts.get(OpflowConstant.OPFLOW_PRODUCING_ROUTING_KEY);

            final String[] _bindingKeys = (String[]) opts.get(OpflowConstant.OPFLOW_CONSUMING_BINDING_KEYS);
            final Boolean _autoBinding = (Boolean) opts.get(OpflowConstant.OPFLOW_CONSUMING_AUTO_BINDING);
            if (!Boolean.FALSE.equals(_autoBinding) && _exchangeName != null) {
                if (_routingKey != null) {
                    bindExchange(_channel, _exchangeName, _queueName, _routingKey);
                }
                if (_bindingKeys != null) {
                    bindExchange(_channel, _exchangeName, _queueName, _bindingKeys);
                }
            }
            
            final String _replyToName;
            String opts_replyToName = (String) opts.get(OpflowConstant.OPFLOW_CONSUMING_REPLY_TO);
            if (opts_replyToName != null) {
                _replyToName = _channel.queueDeclarePassive(opts_replyToName).getQueue();
            } else {
                _replyToName = null;
            }
            
            final Boolean _autoAck;
            if (opts.get(OpflowConstant.OPFLOW_CONSUMING_AUTO_ACK) instanceof Boolean) {
                _autoAck = (Boolean) opts.get(OpflowConstant.OPFLOW_CONSUMING_AUTO_ACK);
            } else {
                _autoAck = Boolean.TRUE;
            }
            
            final Boolean _requeueFailure;
            if (opts.get("requeueFailure") instanceof Boolean) {
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
                    final Map<String, Object> headers = properties.getHeaders();
                    final String routineId = OpflowUtil.getRoutineId(headers, false);
                    final String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers, false);
                    final String routineScope = OpflowUtil.getRoutineScope(headers);

                    final OpflowLogTracer reqTracer = logConsume.branch(CONST.REQUEST_TIME, routineTimestamp)
                            .branch(CONST.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));

                    try {
                        if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .put("appId", properties.getAppId())
                            .put("deliveryTag", envelope.getDeliveryTag())
                            .put("consumerTag", consumerTag)
                            .put("bodyLength", body.length)
                            .text("Request[${requestId}][${requestTime}][x-engine-msg-received] - Consumer[${consumerId}] receives a message (${bodyLength} bytes)")
                            .stringify());

                        if (applicationId == null || applicationId.equals(properties.getAppId())) {
                            if (reqTracer != null && reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                                    .text("Request[${requestId}][${requestTime}] invoke listener.processMessage()")
                                    .stringify());
                            
                            Map<String, String> extras = OpflowObjectTree.buildMap(false)
                                    .put(CONST.AMQP_HEADER_ROUTINE_ID, routineId)
                                    .put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, routineTimestamp)
                                    .put(CONST.AMQP_HEADER_ROUTINE_SCOPE, routineScope)
                                    .toMap();
                            
                            boolean captured = listener.processMessage(body, properties, _replyToName, _channel, consumerTag, extras);
                            
                            if (captured) {
                                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                        .text("Request[${requestId}][${requestTime}][x-engine-delivery-ok] has finished successfully")
                                        .stringify());
                            } else {
                                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                        .text("Request[${requestId}][${requestTime}][x-engine-delivery-skipped] has not matched the criteria, skipped")
                                        .stringify());
                            }
                            
                            if (reqTracer != null && reqTracer.ready(LOG, Level.TRACE)) LOG.trace(reqTracer
                                    .put("deliveryTag", envelope.getDeliveryTag())
                                    .put("consumerTag", consumerTag)
                                    .text("Request[${requestId}][${requestTime}][x-engine-delivery-ack] invoke ACK")
                                    .stringify());
                            
                            invokeAck(envelope, true);
                        } else {
                            if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                                    .put("applicationId", applicationId)
                                    .text("Request[${requestId}][${requestTime}][x-engine-delivery-rejected] has been rejected, mismatched applicationId")
                                    .stringify());
                            invokeAck(envelope, false);
                        }
                    } catch (Exception ex) {
                        // catch ALL of Error here: don't let it harm our service/close the channel
                        if (reqTracer != null && reqTracer.ready(LOG, Level.ERROR)) LOG.error(reqTracer
                                .put("deliveryTag", envelope.getDeliveryTag())
                                .put("consumerTag", consumerTag)
                                .put("exceptionClass", ex.getClass().getName())
                                .put("exceptionMessage", ex.getMessage())
                                .put("autoAck", _autoAck)
                                .put("requeueFailure", _requeueFailure)
                                .text("Request[${requestId}][${requestTime}][x-engine-delivery-exception] has been failed. Service still alive")
                                .stringify());
                        //ex.printStackTrace();
                        invokeAck(envelope, false);
                    }
                }
                
                @Override
                public void handleCancelOk(String consumerTag) {
                    if (logConsume.ready(LOG, Level.INFO)) LOG.info(logConsume
                            .put("consumerTag", consumerTag)
                            .text("Consumer[${consumerId}].consume() - handle CancelOk event")
                            .stringify());
                }
                
                @Override
                public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                    if (logConsume.ready(LOG, Level.INFO)) LOG.info(logConsume
                            .put("consumerTag", consumerTag)
                            .text("Consumer[${consumerId}].consume() - handle ShutdownSignal event")
                            .stringify());
                }
            };
            
            final String _consumerTag = _channel.basicConsume(_queueName, _autoAck, _consumer);
            
            if (logConsume.ready(LOG, Level.INFO)) LOG.info(logConsume
                    .put("queueName", _queueName)
                    .put("consumerTag", _consumerTag)
                    .put("channelNumber", _channel.getChannelNumber())
                    .text("Consumer[${consumerId}].consume() create consumer[${consumerTag}]/queue[${queueName}]")
                    .stringify());
            ConsumerInfo info = new ConsumerInfo(_connection, !_forceNewConnection, 
                    _channel, !_forceNewChannel, _queueName, _fixedQueue, _consumerId, _consumerTag);
            if (OpflowConstant.COMP_ENGINE.equals(owner)) {
                consumerInfos.add(info);
            }
            return info;
        } catch(IOException exception) {
            if (logConsume.ready(LOG, Level.ERROR)) LOG.error(logConsume
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("Consumer[${consumerId}].consume() - has failed")
                    .stringify());
            throw new OpflowOperationException(exception);
        } catch(TimeoutException exception) {
            if (logConsume.ready(LOG, Level.ERROR)) LOG.error(logConsume
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
            if (logCancel.ready(LOG, Level.DEBUG)) LOG.debug(logCancel
                    .text("Consumer[${consumerId}].cancelConsumer() - consumer will be cancelled")
                    .stringify());

            consumerInfo.getChannel().basicCancel(consumerInfo.getConsumerTag());

            if (logCancel.ready(LOG, Level.DEBUG)) LOG.debug(logCancel
                    .text("Consumer[${consumerId}].cancelConsumer() - consumer has been cancelled")
                    .stringify());

            if (!consumerInfo.isSharedConnection() || !consumerInfo.isSharedChannel()) {
                if (consumerInfo.getChannel() != null && consumerInfo.getChannel().isOpen()) {
                    if (logCancel.ready(LOG, Level.DEBUG)) LOG.debug(logCancel
                            .tags("sharedConsumingChannelClosed")
                            .text("Consumer[${consumerId}] shared consumingChannel is closing")
                            .stringify());
                    consumerInfo.getChannel().close();
                }
            }

            if (!consumerInfo.isSharedConnection()) {
                if (consumerInfo.getConnection() != null && consumerInfo.getConnection().isOpen()) {
                    if (logCancel.ready(LOG, Level.DEBUG)) LOG.debug(logCancel
                            .tags("sharedConsumingConnectionClosed")
                            .text("Consumer[${consumerId}] shared consumingConnection is closing")
                            .stringify());
                    consumerInfo.getConnection().close();
                }
            }
        } catch (IOException ex) {
            if (logCancel.ready(LOG, Level.ERROR)) LOG.error(logCancel
                    .put("exceptionClass", ex.getClass().getName())
                    .put("exceptionMessage", ex.getMessage())
                    .text("Consumer[${consumerId}].cancelConsumer() - has failed")
                    .stringify());
        } catch (TimeoutException ex) {
            if (logCancel.ready(LOG, Level.ERROR)) LOG.error(logCancel
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
        
        public ConsumerInfo(
                Connection connection,
                boolean sharedConnection,
                Channel channel,
                boolean sharedChannel,
                String queueName,
                boolean fixedQueue,
                String consumerId,
                String consumerTag
        ) {
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
        int conn = producingConnection != null && producingConnection.isOpen() ? State.CONNECTION_OPENED : State.CONNECTION_CLOSED;
        State state = new State(conn);
        return state;
    }
    
    /**
     * Close this broker.
     *
     * @throws OpflowOperationException if an error is encountered
     */
    @Override
    public void close() {
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
            .text("Engine[${engineId}].close() - close producingChannel, producingConnection")
            .stringify());
        
        synchronized (producingChannelLock) {
            try {
                if (producingChannel != null && producingChannel.isOpen()) {
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedProducingChannelClosed")
                            .text("Engine[${engineId}].close() shared producingChannel is closing")
                            .stringify());
                    producingChannel.close();
                }
            } catch (IOException | TimeoutException exception) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .text("Engine[${engineId}].close() has failed in closing the producingChannel")
                        .stringify());
            } finally {
                producingChannel = null;
            }
            
            synchronized (producingConnectionLock) {
                try {
                    if (producingConnection != null && producingConnection.isOpen()) {
                        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                                .tags("sharedProducingConnectionClosed")
                                .text("Engine[${engineId}].close() shared producingConnection is closing")
                                .stringify());
                        producingConnection.close();
                    }
                } catch (IOException exception) {
                    if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                            .text("Engine[${engineId}].close() has failed in closing the producingConnection")
                            .stringify());
                } finally {
                    producingConnection = null;
                }
            }
        }
        
        if (OpflowConstant.COMP_ENGINE.equals(owner)) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .put("owner", owner)
                    .text("Engine[${engineId}].close() - cancel consumers in [${owner}]")
                    .stringify());
            for(ConsumerInfo consumerInfo: consumerInfos) {
                this.cancelConsumer(consumerInfo);
            }
            consumerInfos.clear();
        }
        
        if (sharedExecutor != null) {
            sharedExecutor.shutdown();
            try {
                if (!sharedExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    sharedExecutor.shutdownNow();
                    sharedExecutor.awaitTermination(5, TimeUnit.SECONDS);
                }
            } catch (InterruptedException ie) {
                sharedExecutor.shutdownNow();
            }
            sharedExecutor = null;
        }
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
            .text("Engine[${engineId}].close() - close consumingChannel, consumingConnection")
            .stringify());
        
        synchronized (consumingChannelLock) {
            try {
                if (consumingChannel != null && consumingChannel.isOpen()) {
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedConsumingChannelClosed")
                            .text("Engine[${engineId}].close() shared consumingChannel is closing")
                            .stringify());
                    consumingChannel.close();
                }
            } catch (IOException | TimeoutException exception) {
                if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                        .text("Engine[${engineId}].close() has failed in closing the consumingChannel")
                        .stringify());
            } finally {
                consumingChannel = null;
            }
            
            synchronized (consumingConnectionLock) {
                try {
                    if (consumingConnection != null && consumingConnection.isOpen()) {
                        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                                .tags("sharedConsumingConnectionClosed")
                                .text("Engine[${engineId}].close() shared consumingConnection is closing")
                                .stringify());
                        consumingConnection.close();
                    }
                } catch (IOException exception) {
                    if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                            .text("Engine[${engineId}].close() has failed in closing the consumingConnection")
                            .stringify());
                } finally {
                    consumingConnection = null;
                }
            }
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
    
    private ExecutorService assertSharedExecutor() {
        sharedExecutor = null;
        if (null != threadPoolType) {
            switch (threadPoolType) {
                case "cached":
                    sharedExecutor = Executors.newCachedThreadPool();
                    break;
                case "fixed":
                    sharedExecutor = Executors.newFixedThreadPool(threadPoolSize);
                    break;
                case "single":
                    sharedExecutor = Executors.newSingleThreadExecutor();
                    break;
                case "single-scheduled":
                    sharedExecutor = Executors.newSingleThreadScheduledExecutor();
                    break;
                case "scheduled":
                    sharedExecutor = Executors.newScheduledThreadPool(threadPoolSize);
                    break;
                default:
                    break;
            }
        }
        return sharedExecutor;
    }
    
    private String getConnectionId(Connection conn) {
        return (conn != null) ? conn.getId() : null;
    }
    
    private Connection getProducingConnection() throws IOException, TimeoutException {
        if (producingConnection == null || !producingConnection.isOpen()) {
            synchronized (producingConnectionLock) {
                if (producingConnection == null || !producingConnection.isOpen()) {
                    producingConnectionId = OpflowUUID.getBase64ID();
                    producingConnection = factory.newConnection();
                    producingConnection.setId(producingConnectionId);
                    producingConnection.addBlockedListener(new BlockedListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();

                        @Override
                        public void handleBlocked(String reason) throws IOException {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", producingConnectionId)
                                    .text("Engine[${engineId}] producingConnection[${connectionId}] has been blocked")
                                    .stringify());
                            synchronized (producingBlockedListenerLock) {
                                if (producingBlockedListener != null) {
                                    producingBlockedListener.handleBlocked(reason);
                                }
                            }
                        }

                        @Override
                        public void handleUnblocked() throws IOException {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", producingConnectionId)
                                    .text("Engine[${engineId}] producingConnection[${connectionId}] has been unblocked")
                                    .stringify());
                            synchronized (producingBlockedListenerLock) {
                                if (producingBlockedListener != null) {
                                    producingBlockedListener.handleUnblocked();
                                }
                            }
                        }
                    });
                    producingConnection.addShutdownListener(new ShutdownListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();
                        @Override
                        public void shutdownCompleted(ShutdownSignalException sse) {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", producingConnectionId)
                                    .text("Engine[${engineId}] producingConnection[${connectionId}] has been shutdown")
                                    .stringify());
                        }
                    });
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedProducingConnectionCreated")
                            .put("connectionId", producingConnectionId)
                            .text("Engine[${engineId}]shared producingConnection[${connectionId}] is created")
                            .stringify(true));
                    measurer.updateEngineConnection(factory, owner, "producing", OpflowPromMeasurer.GaugeAction.INC);
                }
            }
        }
        return producingConnection;
    }
    
    private Channel getProducingChannel() throws IOException, TimeoutException {
        if (producingChannel == null || !producingChannel.isOpen()) {
            synchronized (producingChannelLock) {
                if (producingChannel == null || !producingChannel.isOpen()) {
                    producingChannel = getProducingConnection().createChannel();
                    producingChannel.addShutdownListener(new ShutdownListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();
                        @Override
                        public void shutdownCompleted(ShutdownSignalException sse) {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("channelNumber", producingChannel.getChannelNumber())
                                    .text("Engine[${engineId}] producingChannel[${channelNumber}] has been shutdown")
                                    .stringify());
                        }
                    });
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedProducingChannelCreated")
                            .put("channelNumber", producingChannel.getChannelNumber())
                            .text("Engine[${engineId}] shared producingChannel[${channelNumber}] is created")
                            .stringify());
                }
            }
        }
        return producingChannel;
    }
    
    public void setProducingBlockedListener(BlockedListener producingBlockedListener) {
        synchronized (producingBlockedListenerLock) {
            this.producingBlockedListener = producingBlockedListener;
        }
    }
    
    private Connection getConsumingConnection(boolean forceNewConnection) throws IOException, TimeoutException {
        if (forceNewConnection) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .tags("privateConsumingConnectionCreated")
                    .text("Engine[${engineId}] private consumingConnection is created")
                    .stringify());
            return factory.newConnection();
        }
        if (consumingConnection == null || !consumingConnection.isOpen()) {
            synchronized (consumingConnectionLock) {
                if (consumingConnection == null || !consumingConnection.isOpen()) {
                    if (sharedExecutor == null) {
                        sharedExecutor = assertSharedExecutor();
                        if (sharedExecutor != null) {
                            factory.setSharedExecutor(sharedExecutor);
                        }
                    }
                    consumingConnectionId = OpflowUUID.getBase64ID();
                    consumingConnection = factory.newConnection();
                    consumingConnection.setId(consumingConnectionId);
                    consumingConnection.addBlockedListener(new BlockedListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();
                        
                        @Override
                        public void handleBlocked(String reason) throws IOException {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", consumingConnectionId)
                                    .text("Engine[${engineId}] consumingConnection[${connectionId}] has been blocked")
                                    .stringify());
                            synchronized (consumingBlockedListenerLock) {
                                if (consumingBlockedListener != null) {
                                    consumingBlockedListener.handleBlocked(reason);
                                }
                            }
                        }

                        @Override
                        public void handleUnblocked() throws IOException {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", consumingConnectionId)
                                    .text("Engine[${engineId}] consumingConnection[${connectionId}] has been unblocked")
                                    .stringify());
                            synchronized (consumingBlockedListenerLock) {
                                if (consumingBlockedListener != null) {
                                    consumingBlockedListener.handleUnblocked();
                                }
                            }
                        }
                    });
                    consumingConnection.addShutdownListener(new ShutdownListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();
                        @Override
                        public void shutdownCompleted(ShutdownSignalException sse) {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("connectionId", consumingConnectionId)
                                    .text("Engine[${engineId}] consumingConnection[${connectionId}] has been shutdown")
                                    .stringify());
                        }
                    });
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedConsumingConnectionCreated")
                            .put("connectionId", consumingConnectionId)
                            .text("Engine[${engineId}] shared consumingConnection[${connectionId}] is created")
                            .stringify(true));
                    measurer.updateEngineConnection(factory, owner, "consuming", OpflowPromMeasurer.GaugeAction.INC);
                }
            }
        }
        return consumingConnection;
    }
    
    private Channel getConsumingChannel(boolean forceNewConnection, boolean forceNewChannel) throws IOException, TimeoutException {
        if (forceNewConnection) {
            return getConsumingConnection(forceNewConnection).createChannel();
        }
        if (forceNewChannel) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .tags("privateConsumingChannelCreated")
                    .text("Engine[${engineId}] private consumingChannel is created")
                    .stringify());
            return getConsumingConnection(false).createChannel();
        }
        if (consumingChannel == null || !consumingChannel.isOpen()) {
            synchronized (consumingChannelLock) {
                if (consumingChannel == null || !consumingChannel.isOpen()) {
                    consumingChannel = getConsumingConnection(false).createChannel();
                    consumingChannel.addShutdownListener(new ShutdownListener() {
                        private final OpflowLogTracer localLog = logTracer.copy();
                        @Override
                        public void shutdownCompleted(ShutdownSignalException sse) {
                            if (localLog.ready(LOG, Level.INFO)) LOG.info(localLog
                                    .put("channelNumber", consumingChannel.getChannelNumber())
                                    .text("Engine[${engineId}] consumingChannel[${channelNumber}] has been shutdown")
                                    .stringify());
                        }
                    });
                    if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                            .tags("sharedConsumingChannelCreated")
                            .text("Engine[${engineId}] shared consumingChannel is created")
                            .stringify());
                }
            }
        }
        return consumingChannel;
    }
    
    public void setConsumingBlockedListener(BlockedListener consumingBlockedListener) {
        synchronized (consumingBlockedListenerLock) {
            this.consumingBlockedListener = consumingBlockedListener;
        }
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String _routingKey) throws IOException {
        bindExchange(_channel, _exchangeName, _queueName, new String[] { _routingKey });
    }
    
    private void bindExchange(Channel _channel, String _exchangeName, String _queueName, String[] keys) throws IOException {
        _channel.exchangeDeclarePassive(_exchangeName);
        _channel.queueDeclarePassive(_queueName);
        for (String _routingKey : keys) {
            _channel.queueBind(_queueName, _exchangeName, _routingKey);
            if (logTracer.ready(LOG, Level.TRACE)) LOG.trace(logTracer
                    .put("exchangeName", _exchangeName)
                    .put("queueName", _queueName)
                    .put("routingKey", _routingKey)
                    .text("Engine[${engineId}] binds Exchange[${exchangeName}] to Queue[${queueName}] with routingKey[${routingKey}]")
                    .stringify());
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        measurer.updateComponentInstance(OpflowConstant.COMP_ENGINE, componentId, OpflowPromMeasurer.GaugeAction.DEC);
    }
    
    public interface Listener {
        public boolean processMessage(
                byte[] content,
                AMQP.BasicProperties properties,
                String queueName,
                Channel channel,
                String consumerTag,
                Map<String, String> extras) throws IOException;
    }
    
    public static class Message {

        private final byte[] body;
        private final Map<String, Object> headers;

        public final static Message EMPTY = new Message();
        public final static Message ERROR = new Message(null, OpflowObjectTree.buildMap(false).put("status", "failed").toMap());

        private Message() {
            body = null;
            headers = null;
        }

        public Message(byte[] body, Map<String, Object> headers) {
            this.body = body;
            this.headers = headers;
        }

        public byte[] getBody() {
            return body;
        }

        public String getBodyAsString() {
            if (body == null) return null;
            return OpflowUtil.getString(body);
        }

        public Map<String, Object> getHeaders() {
            return headers;
        }
    }
}
