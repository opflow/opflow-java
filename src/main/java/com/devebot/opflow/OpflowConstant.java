package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowConstant {
    public final String FRAMEWORK_ID = "opflow";
    
    public final String INSTANCE_ID = "instanceId";
    public final String COMPONENT_ID = "componentId";
    public final String COMPONENT_TYPE = "componentType";

    public final String ROUTINE_ID = "routineId";
    public final String REQUEST_ID = "requestId";
    public final String REQUEST_TIME = "requestTime";
    public final String REQUEST_TAGS = "requestTags";

    public final String COMPNAME_COMMANDER = "commander";
    public final String COMPNAME_SERVERLET = "serverlet";
    public final String COMPNAME_PUBLISHER = "publisher";
    public final String COMPNAME_CONFIGURER = "configurer";
    public final String COMPNAME_SUBSCRIBER = "subscriber";
    public final String COMPNAME_MEASURER = "measurer";
    public final String COMPNAME_PROM_EXPORTER = "promExporter";
    public final String COMPNAME_RESTRICTOR = "restrictor";
    public final String COMPNAME_REQ_EXTRACTOR = "reqExtractor";
    public final String COMPNAME_SPEED_METER = "speedMeter";
    public final String COMPNAME_RPC_MASTER = "rpcMaster";
    public final String COMPNAME_RPC_WORKER = "rpcWorker";
    public final String COMPNAME_NATIVE_WORKER = "ReservedWorker";
    public final String COMPNAME_REMOTE_WORKER = "DetachedWorker";
    public final String COMPNAME_RPC_WATCHER = "rpcWatcher";
    public final String COMPNAME_REST_SERVER = "restServer";

    public final String RPC_INVOCATION_FLOW_PUBLISHER = "publisher";
    public final String RPC_INVOCATION_FLOW_RPC_MASTER = "master";
    public final static String RPC_INVOCATION_FLOW_DETACHED_WORKER = "detached_worker";
    public final static String RPC_INVOCATION_FLOW_RESERVED_WORKER = "reserved_worker";

    public final static String REQUEST_TRACER_NAME = "reqTracer";

    private OpflowConstant() {
    }

    private static final Object LOCK = new Object();
    private static OpflowConstant instance = null;

    public static OpflowConstant CURRENT() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new OpflowConstant();
                }
            }
        }
        return instance;
    }
}
