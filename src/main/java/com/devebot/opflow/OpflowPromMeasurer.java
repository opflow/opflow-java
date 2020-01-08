package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {

    public final static String DEFAULT_PROM_EXPORTER_PORT_VAL = "9450";
    public final static String DEFAULT_PROM_EXPORTER_PORT_KEY = "opflow.exporter.port";
    public final static String DEFAULT_PROM_EXPORTER_PORT_ENV = "OPFLOW_EXPORTER_PORT";

    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_VAL = "localhost:9091";
    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_KEY = "opflow.pushgateway.addr";
    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_ENV = "OPFLOW_PUSHGATEWAY_ADDR";
    public static final String DEFAULT_PROM_PUSHGATEWAY_JOBNAME = "opflow-push-gateway";

    public static enum GaugeAction {
        INC,
        DEC;
    }
    
    public abstract void updateComponentInstance(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void removeComponentInstance(String instanceType, String instanceId);
    
    public abstract void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action);
    
    public abstract void updateActiveChannel(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void countRpcInvocation(String moduleName, String eventName, String routineId, String status);
    
    public abstract double getRpcInvocationTotal(String moduleName, String eventName);
    
    private static OpflowPromMeasurer instance;

    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return getInstance(null);
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (instance == null) {
            instance = new OpflowPromExporter();
        }
        return instance;
    }
    
    static class DevNull extends OpflowPromMeasurer {

        @Override
        public void updateComponentInstance(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void removeComponentInstance(String instanceType, String instanceId) {
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
        }

        @Override
        public void updateActiveChannel(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineId, String status) {
        }
        
        @Override
        public double getRpcInvocationTotal(String moduleName, String eventName) {
            return 0;
        }
    }
    
    public static final OpflowPromMeasurer NULL = new DevNull();
}
