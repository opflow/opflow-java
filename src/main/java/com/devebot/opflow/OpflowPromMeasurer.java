package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.ConnectionFactory;

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
    
    public abstract void changeComponentInstance(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void removeComponentInstance(String instanceType, String instanceId);
    
    public abstract void incEngineConnectionGauge(ConnectionFactory factory, String connectionType);
    
    public abstract void decEngineConnectionGauge(ConnectionFactory factory, String connectionType);
    
    public abstract void changeActiveChannel(String instanceType, String instanceId, GaugeAction action);
    
    public abstract void incRpcInvocationEvent(String module_name, String engineId, String routineId, String status);
    
    private static OpflowPromMeasurer instance;

    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        if (instance == null) {
            instance = new OpflowPromExporter();
        }
        return instance;
    }
    
    static class DevNull extends OpflowPromMeasurer {

        @Override
        public void changeComponentInstance(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void removeComponentInstance(String instanceType, String instanceId) {
        }

        @Override
        public void incEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        }

        @Override
        public void decEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        }

        @Override
        public void changeActiveChannel(String instanceType, String instanceId, GaugeAction action) {
        }

        @Override
        public void incRpcInvocationEvent(String module_name, String engineId, String routineId, String status) {
        }
    }
    
    public static final OpflowPromMeasurer NULL = new DevNull();
}
