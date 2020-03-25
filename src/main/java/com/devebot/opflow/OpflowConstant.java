package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowEnvTool;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 * @author acegik
 */
public class OpflowConstant {
    private final static OpflowEnvTool ENVTOOL = OpflowEnvTool.instance;

    public enum Protocol { AMQP, HTTP };

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OPFLOW

    public final String FRAMEWORK_ID = "opflow";

    public final String INSTANCE_ID = "instanceId";
    public final String COMPONENT_ID = "componentId";
    public final String COMPONENT_TYPE = "componentType";
    public final String RPC_MASTER_ID = "rpcMasterId";
    public final String RPC_WORKER_ID = "rpcWorkerId";

    public final String REQUEST_ID = "requestId";
    public final String REQUEST_TIME = "requestTime";

    public final static String ROUTINE_ID = "routineId";
    public final static String ROUTINE_TIMESTAMP = "routineTimestamp";
    public final static String ROUTINE_SIGNATURE = "routineSignature";
    public final static String ROUTINE_SCOPE = "routineScope";

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OPFLOW COMPONENTS

    public final static String COMP_ENGINE = "engine";
    public final static String COMP_COMMANDER = "commander";
    public final static String COMP_SERVERLET = "serverlet";
    public final static String COMP_PUBLISHER = "publisher";
    public final static String COMP_SUBSCRIBER = "subscriber";
    public final static String COMP_MEASURER = "measurer";
    public final static String COMP_PROM_EXPORTER = "promExporter";
    public final static String COMP_RESTRICTOR = "restrictor";
    public final static String COMP_REQ_EXTRACTOR = "reqExtractor";
    public final static String COMP_SPEED_METER = "speedMeter";

    public final static String COMP_CFG_AMQP_MASTER = "rpcMaster";
    public final static String COMP_CFG_AMQP_WORKER = "rpcWorker";

    public final static String COMP_RPC_AMQP_MASTER = "amqpMaster";
    public final static String COMP_RPC_AMQP_WORKER = "amqpWorker";

    public final static String COMP_RPC_HTTP_MASTER = "httpMaster";
    public final static String COMP_RPC_HTTP_WORKER = "httpWorker";

    public final static String COMP_DISCOVERY_CLIENT = "discoveryClient";
    public final static String COMP_NATIVE_WORKER = "nativeWorker";
    public final static String COMP_REMOTE_AMQP_WORKER = "amqpWorker";
    public final static String COMP_REMOTE_HTTP_WORKER = "httpWorker";
    public final static String COMP_RPC_WATCHER = "rpcWatcher";
    public final static String COMP_RPC_OBSERVER = "rpcObserver";
    public final static String COMP_REST_SERVER = "restServer";

    public final static String INFO_SECTION_RUNTIME = "miscellaneous";
    public final static String INFO_SECTION_SOURCE_CODE = "source-code-info";

    public final static String METHOD_INVOCATION_FLOW_NATIVE = "native";
    public final static String METHOD_INVOCATION_FLOW_PUBSUB = "pubsub";
    public final static String METHOD_INVOCATION_FLOW_RPC = "rpc";
    public final static String METHOD_INVOCATION_REMOTE_HTTP_WORKER = "remote_http";
    public final static String METHOD_INVOCATION_REMOTE_AMQP_WORKER = "remote_amqp";
    public final static String METHOD_INVOCATION_NATIVE_WORKER = "backup_method";

    public final static String PARAM_NATIVE_WORKER_ENABLED = "nativeWorkerEnabled";

    // engine - rabbitMQ
    public final static String OPFLOW_COMMON_APP_ID = "applicationId";
    public final static String OPFLOW_COMMON_INSTANCE_OWNER = "mode";
    public final static String OPFLOW_COMMON_ACTIVE = "active";
    public final static String OPFLOW_COMMON_AUTORUN = "autorun";
    public final static String OPFLOW_COMMON_ENABLED = "enabled";
    public final static String OPFLOW_COMMON_VERBOSE = "verbose";
    public final static String OPFLOW_COMMON_INTERVAL = "interval";
    public final static String OPFLOW_COMMON_COUNT = "count";
    public final static String OPFLOW_COMMON_LENGTH = "length";
    public final static String OPFLOW_COMMON_STRICT = "strictMode";
    public final static String OPFLOW_COMMON_HOST = "host";
    public final static String OPFLOW_COMMON_HOSTNAME = "hostname";
    public final static String OPFLOW_COMMON_PORTS = "ports";
    public final static String OPFLOW_COMMON_ADDRESS = "address";
    public final static String OPFLOW_COMMON_CREDENTIALS = "credentials";
    public final static String OPFLOW_COMMON_CONGESTIVE = "congestive";
    public final static String OPFLOW_COMMON_PROTOCOL = "protocol";
    public final static String OPFLOW_COMMON_THREAD_COUNT = "threadCount";
    public final static String OPFLOW_COMMON_CURRENT_TIMESTAMP = "currentTime";
    public final static String OPFLOW_COMMON_START_TIMESTAMP = "startTime";
    public final static String OPFLOW_COMMON_END_TIMESTAMP = "endTime";
    public final static String OPFLOW_COMMON_ELAPSED_TIME = "elapsedTime";
    public final static String OPFLOW_COMMON_UPTIME = "uptime";
    public final static String OPFLOW_COMMON_CHANNEL = "connection";

    public final static String OPFLOW_REQ_EXTRACTOR_CLASS_NAME = "getRequestIdClassName";
    public final static String OPFLOW_REQ_EXTRACTOR_METHOD_NAME = "getRequestIdMethodName";
    public final static String OPFLOW_REQ_EXTRACTOR_AUTO_UUID = "uuidIfNotFound";
    
    public final static String OPFLOW_RPC_MONITOR_ID = "monitorId";
    public final static String OPFLOW_RPC_MONITOR_ENABLED = "monitorEnabled";
    public final static String OPFLOW_RPC_MONITOR_INTERVAL = "monitorInterval";
    public final static String OPFLOW_RPC_MONITOR_TIMEOUT = "monitorTimeout";

    public final static String OPFLOW_RESTRICT_PAUSE_ENABLED = "pauseEnabled";
    public final static String OPFLOW_RESTRICT_PAUSE_TIMEOUT = "pauseTimeout";
    public final static String OPFLOW_RESTRICT_PAUSE_STATUS = "pauseStatus";
    public final static String OPFLOW_RESTRICT_PAUSE_ELAPSED_TIME = "pauseElapsedTime";
    public final static String OPFLOW_RESTRICT_PAUSE_DURATION = "pauseDuration";

    public final static String OPFLOW_RESTRICT_SEMAPHORE_ENABLED = "semaphoreEnabled";
    public final static String OPFLOW_RESTRICT_SEMAPHORE_TIMEOUT = "semaphoreTimeout";
    public final static String OPFLOW_RESTRICT_SEMAPHORE_PERMITS = "semaphoreLimit";
    public final static String OPFLOW_RESTRICT_SEMAPHORE_FREE_PERMITS = "semaphoreFreePermits";
    public final static String OPFLOW_RESTRICT_SEMAPHORE_USED_PERMITS = "semaphoreUsedPermits";

    public final static String OPFLOW_PRODUCING_EXCHANGE_NAME = "exchangeName";
    public final static String OPFLOW_PRODUCING_EXCHANGE_TYPE = "exchangeType";
    public final static String OPFLOW_PRODUCING_EXCHANGE_DURABLE = "exchangeDurable";
    public final static String OPFLOW_PRODUCING_ROUTING_KEY = "routingKey";

    public final static String OPFLOW_CONSUMING_QUEUE_NAME = "queueName";
    public final static String OPFLOW_CONSUMING_QUEUE_AUTO_DELETE = "autoDelete";
    public final static String OPFLOW_CONSUMING_QUEUE_DURABLE = "durable";
    public final static String OPFLOW_CONSUMING_QUEUE_EXCLUSIVE = "exclusive";
    public final static String OPFLOW_CONSUMING_BINDING_KEYS = "otherKeys";
    public final static String OPFLOW_CONSUMING_PREFETCH_COUNT = "prefetchCount";
    public final static String OPFLOW_CONSUMING_AUTO_ACK = "autoAck";
    public final static String OPFLOW_CONSUMING_AUTO_BINDING = "binding";
    public final static String OPFLOW_CONSUMING_REPLY_TO = "replyTo";
    public final static String OPFLOW_CONSUMING_CONSUMER_ID = "consumerId";
    public final static String OPFLOW_CONSUMING_CONSUMER_LIMIT = "consumerLimit";

    // publisher - subscriber

    public final static String OPFLOW_PUBSUB_EXCHANGE_NAME = "exchangeName";
    public final static String OPFLOW_PUBSUB_EXCHANGE_TYPE = "exchangeType";
    public final static String OPFLOW_PUBSUB_EXCHANGE_DURABLE = "exchangeDurable";
    public final static String OPFLOW_PUBSUB_ROUTING_KEY = "routingKey";

    public final static String OPFLOW_PUBSUB_BINDING_KEYS = "otherKeys";
    public final static String OPFLOW_PUBSUB_QUEUE_NAME = "subscriberName";
    public final static String OPFLOW_PUBSUB_QUEUE_DURABLE = "queueDurable";
    public final static String OPFLOW_PUBSUB_QUEUE_EXCLUSIVE = "queueExclusive";
    public final static String OPFLOW_PUBSUB_QUEUE_AUTO_DELETE = "queueAutoDelete";
    public final static String OPFLOW_PUBSUB_PREFETCH_COUNT = "prefetchCount";
    public final static String OPFLOW_PUBSUB_AUTO_BINDING = "binding";
    public final static String OPFLOW_PUBSUB_REPLY_TO = "replyTo";
    public final static String OPFLOW_PUBSUB_CONSUMER_ID = "consumerId";
    public final static String OPFLOW_PUBSUB_CONSUMER_LIMIT = "subscriberLimit";
    public final static String OPFLOW_PUBSUB_REDELIVERED_LIMIT = "redeliveredLimit";
    public final static String OPFLOW_PUBSUB_TRASH_NAME = "recyclebinName";

    // producer - master
    public final static String OPFLOW_DISPATCH_EXCHANGE_NAME = "exchangeName";
    public final static String OPFLOW_DISPATCH_EXCHANGE_TYPE = "exchangeType";
    public final static String OPFLOW_DISPATCH_EXCHANGE_DURABLE = "exchangeDurable";
    public final static String OPFLOW_DISPATCH_ROUTING_KEY = "routingKey";

    public final static String OPFLOW_RESPONSE_QUEUE_NAME = "responseName";
    public final static String OPFLOW_RESPONSE_QUEUE_SUFFIX = "responseQueueSuffix";
    public final static String OPFLOW_RESPONSE_QUEUE_AUTO_DELETE = "responseAutoDelete";
    public final static String OPFLOW_RESPONSE_QUEUE_DURABLE = "responseDurable";
    public final static String OPFLOW_RESPONSE_QUEUE_EXCLUSIVE = "responseExclusive";
    public final static String OPFLOW_RESPONSE_PREFETCH_COUNT = "prefetchCount";

    // consumer - worker
    public final static String OPFLOW_INCOMING_QUEUE_NAME = "operatorName";
    public final static String OPFLOW_INCOMING_QUEUE_AUTO_DELETE = "operatorAutoDelete";
    public final static String OPFLOW_INCOMING_QUEUE_DURABLE = "operatorDurable";
    public final static String OPFLOW_INCOMING_QUEUE_EXCLUSIVE = "operatorExclusive";
    public final static String OPFLOW_INCOMING_BINDING_KEYS = "otherKeys";
    public final static String OPFLOW_INCOMING_PREFETCH_COUNT = "prefetchCount";

    public final static String OPFLOW_OUTGOING_EXCHANGE_NAME = "outgoingExchangeName";
    public final static String OPFLOW_OUTGOING_EXCHANGE_TYPE = "outgoingExchangeType";
    public final static String OPFLOW_OUTGOING_EXCHANGE_DURABLE = "outgoingExchangeDurable";
    public final static String OPFLOW_OUTGOING_ROUTING_KEY = "outgoingRoutingKey";

    public final static String OPFLOW_RES_HEADER_WORKER_ID = "o-rpcWorkerId";
    public final static String OPFLOW_RES_HEADER_HTTP_ADDRESS = "o-httpAddress";
    public final static String OPFLOW_RES_HEADER_AMQP_PATTERN = "o-bindingKey";

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RPC HTTP HEADERS

    public final static String HTTP_HEADER_ROUTINE_ID = "x-oxId";
    public final static String HTTP_HEADER_ROUTINE_TIMESTAMP = "x-oxTimestamp";
    public final static String HTTP_HEADER_ROUTINE_SCOPE = "x-oxScope";
    public final static String HTTP_HEADER_ROUTINE_SIGNATURE = "x-oxSignature";

    public final static String HTTP_MASTER_PARAM_PULL_TIMEOUT = "readTimeout";
    public final static String HTTP_MASTER_PARAM_PUSH_TIMEOUT = "writeTimeout";
    public final static String HTTP_MASTER_PARAM_CALL_TIMEOUT = "callTimeout";
    
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ AMQP PARAMETERS

    public final static String AMQP_CONARG_URI = "uri";
    public final static String AMQP_CONARG_HOST = "host";
    public final static String AMQP_CONARG_PORT = "port";
    public final static String AMQP_CONARG_VHOST = "virtualHost";
    public final static String AMQP_CONARG_USERNAME = "username";
    public final static String AMQP_CONARG_PASSWORD = "password";

    public final static String AMQP_CONARG_REQUESTED_CHANNEL_MAX = "channelMax";
    public final static String AMQP_CONARG_REQUESTED_FRAME_MAX = "frameMax";
    public final static String AMQP_CONARG_REQUESTED_HEARTBEAT = "heartbeat";

    public final static String AMQP_CONARG_AUTOMATIC_RECOVERY_ENABLED = "automaticRecoveryEnabled";
    public final static String AMQP_CONARG_TOPOLOGY_RECOVERY_ENABLED = "topologyRecoveryEnabled";
    public final static String AMQP_CONARG_NETWORK_RECOVERY_INTERVAL = "networkRecoveryInterval";

    public final static String AMQP_CONARG_PKCS12_FILE = "pkcs12File";
    public final static String AMQP_CONARG_PKCS12_PASSPHRASE = "pkcs12Passphrase";
    public final static String AMQP_CONARG_CA_CERT_FILE = "caCertFile";
    public final static String AMQP_CONARG_SERVER_CERT_FILE = "serverCertFile";
    public final static String AMQP_CONARG_TRUST_STORE_FILE = "trustStoreFile";
    public final static String AMQP_CONARG_TRUST_PASSPHRASE = "trustPassphrase";
    
    public final static String AMQP_CONARG_SHARED_THREAD_POOL_TYPE = "threadPoolType";
    public final static String AMQP_CONARG_SHARED_THREAD_POOL_SIZE = "threadPoolSize";

    public final static String AMQP_PARAM_APP_ID = "appId";
    public final static String AMQP_PARAM_MESSAGE_TTL = "expiration";
    public final static String AMQP_PARAM_REPLY_TO = "replyTo";

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OPFLOW/AMQP PROTOCOL

    public final String AMQP_PROTOCOL_VERSION;
    public final String AMQP_HEADER_PROTOCOL_VERSION;
    public final String AMQP_HEADER_ROUTINE_ID;
    public final String AMQP_HEADER_ROUTINE_TIMESTAMP;
    public final String AMQP_HEADER_ROUTINE_SIGNATURE;
    public final String AMQP_HEADER_ROUTINE_SCOPE;
    public final String AMQP_HEADER_ROUTINE_TAGS;
    public final String AMQP_HEADER_PROGRESS_ENABLED;
    public final String AMQP_HEADER_CONSUMER_ID;
    public final String AMQP_HEADER_CONSUMER_TAG;
    public final String AMQP_HEADER_RETURN_STATUS;

    public final boolean LEGACY_HEADER_APPLIED;
    public final boolean LEGACY_HEADER_ENABLED;

    public final static String LEGACY_HEADER_ROUTINE_ID = "requestId";
    public final static String LEGACY_HEADER_ROUTINE_TIMESTAMP = "requestTime";
    public final static String LEGACY_HEADER_ROUTINE_SIGNATURE = "routineId";
    public final static String LEGACY_HEADER_ROUTINE_SCOPE = "messageScope";
    public final static String LEGACY_HEADER_ROUTINE_TAGS = "requestTags";

    public final boolean LEGACY_ROUTINE_PINGPONG_APPLIED;
    public final static String OPFLOW_ROUTINE_PINGPONG_ALIAS = "opflow_routine_ping_ball_pong";

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ CONSTRUCTORS

    private OpflowConstant() {
        AMQP_PROTOCOL_VERSION = ENVTOOL.getEnvironVariable("OPFLOW_AMQP_PROTOCOL_VERSION", "1");
        AMQP_HEADER_PROTOCOL_VERSION = "oxVersion";
        switch (AMQP_PROTOCOL_VERSION) {
            case "1":
                AMQP_HEADER_ROUTINE_ID = "oxId";
                AMQP_HEADER_ROUTINE_TIMESTAMP = "oxTimestamp";
                AMQP_HEADER_ROUTINE_SIGNATURE = "oxSignature";
                AMQP_HEADER_ROUTINE_SCOPE = "oxScope";
                AMQP_HEADER_ROUTINE_TAGS = "oxTags";
                break;
            default:
                AMQP_HEADER_ROUTINE_ID = LEGACY_HEADER_ROUTINE_ID;
                AMQP_HEADER_ROUTINE_TIMESTAMP = LEGACY_HEADER_ROUTINE_TIMESTAMP;
                AMQP_HEADER_ROUTINE_SIGNATURE = LEGACY_HEADER_ROUTINE_SIGNATURE;
                AMQP_HEADER_ROUTINE_SCOPE = LEGACY_HEADER_ROUTINE_SCOPE;
                AMQP_HEADER_ROUTINE_TAGS = LEGACY_HEADER_ROUTINE_TAGS;
                break;
        }
        AMQP_HEADER_PROGRESS_ENABLED = "progressEnabled";
        AMQP_HEADER_CONSUMER_ID = "rpcWorkerId";
        AMQP_HEADER_CONSUMER_TAG = "consumerTag";
        AMQP_HEADER_RETURN_STATUS = "status";
        // Legacy supports for header names and pingpong routine signature
        LEGACY_HEADER_ENABLED = !"false".equals(ENVTOOL.getEnvironVariable("OPFLOW_LEGACY_SUPPORTED", null));
        LEGACY_HEADER_APPLIED = LEGACY_HEADER_ENABLED && !"0".equals(AMQP_PROTOCOL_VERSION);
        LEGACY_ROUTINE_PINGPONG_APPLIED = !"false".equals(ENVTOOL.getEnvironVariable("OPFLOW_LEGACY_PINGPONG", null));
    }

    public Map<String, String> getProtocolInfo() {
        Map<String, String> info = new LinkedHashMap<>();
        info.put("AMQP_PROTOCOL_VERSION", AMQP_PROTOCOL_VERSION);
        info.put("AMQP_HEADER_ROUTINE_ID", AMQP_HEADER_ROUTINE_ID);
        info.put("AMQP_HEADER_ROUTINE_TIMESTAMP", AMQP_HEADER_ROUTINE_TIMESTAMP);
        info.put("AMQP_HEADER_ROUTINE_SIGNATURE", AMQP_HEADER_ROUTINE_SIGNATURE);
        info.put("AMQP_HEADER_ROUTINE_SCOPE", AMQP_HEADER_ROUTINE_SCOPE);
        info.put("AMQP_HEADER_ROUTINE_TAGS", AMQP_HEADER_ROUTINE_TAGS);
        if (LEGACY_HEADER_APPLIED) {
            info.put("LEGACY_HEADER_ROUTINE_ID", LEGACY_HEADER_ROUTINE_ID);
            info.put("LEGACY_HEADER_ROUTINE_TIMESTAMP", LEGACY_HEADER_ROUTINE_TIMESTAMP);
            info.put("LEGACY_HEADER_ROUTINE_SIGNATURE", LEGACY_HEADER_ROUTINE_SIGNATURE);
            info.put("LEGACY_HEADER_ROUTINE_SCOPE", LEGACY_HEADER_ROUTINE_SCOPE);
        }
        return info;
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ SINGLETON

    private static OpflowConstant instance = null;

    public static OpflowConstant CURRENT() {
        if (instance == null) {
            synchronized (OpflowConstant.class) {
                if (instance == null) {
                    instance = new OpflowConstant();
                }
            }
        }
        return instance;
    }
}
