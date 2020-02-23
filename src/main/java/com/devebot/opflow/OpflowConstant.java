package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowConstant {

    public final String INSTANCE_ID = "instanceId";
    public final String COMPONENT_ID;
    public final String COMPONENT_TYPE;

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
    public final String COMPNAME_RPC_WATCHER = "rpcWatcher";
    public final String COMPNAME_REST_SERVER = "restServer";
    
    private OpflowConstant() {
        COMPONENT_ID = "componentId";
        COMPONENT_TYPE = "componentType";
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
