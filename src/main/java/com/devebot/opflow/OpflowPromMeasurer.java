package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Map;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {

    public static enum GaugeAction { INC, DEC }
    
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
            instance = new OpflowPromExporter(kwargs);
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
