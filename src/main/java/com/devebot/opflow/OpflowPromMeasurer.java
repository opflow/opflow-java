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
    
    private static PipeMeasurer instance = new PipeMeasurer();

    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return instance;
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (OpflowUtil.isComponentEnabled(kwargs)) {
            instance.setShadow(new OpflowPromExporter(kwargs));
        }
        return instance;
    }
    
    static class PipeMeasurer extends OpflowPromMeasurer {

        private OpflowPromMeasurer shadow = null;

        private long rpcInvocation_Master = 0;
        private long rpcInvocation_DirectWorker = 0;
        private long rpcInvocation_RemoteWorker = 0;

        public PipeMeasurer() {
        }

        public PipeMeasurer(OpflowPromMeasurer shadow) {
            this.shadow = shadow;
        }

        public void setShadow(OpflowPromMeasurer shadow) {
            this.shadow = shadow;
        }

        @Override
        public void updateComponentInstance(String instanceType, String instanceId, GaugeAction action) {
            if (shadow != null) {
                shadow.removeComponentInstance(instanceType, instanceId);
            }
        }

        @Override
        public void removeComponentInstance(String instanceType, String instanceId) {
            if (shadow != null) {
                shadow.removeComponentInstance(instanceType, instanceId);
            }
        }

        @Override
        public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(factory, connectionType, action);
            }
        }

        @Override
        public void updateActiveChannel(String instanceType, String instanceId, GaugeAction action) {
            if (shadow != null) {
                shadow.updateActiveChannel(instanceType, instanceId, action);
            }
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineId, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(moduleName, eventName, routineId, status);
            }
            if ("commander".equals(moduleName)) {
                switch (eventName) {
                    case "master":
                        rpcInvocation_Master++;
                        break;
                    case "direct_worker":
                        rpcInvocation_DirectWorker++;
                        break;
                    case "remote_worker":
                        rpcInvocation_RemoteWorker++;
                        break;
                    default:
                        break;
                }
            }
        }
        
        @Override
        public double getRpcInvocationTotal(String moduleName, String eventName) {
            if ("commander".equals(moduleName)) {
                switch (eventName) {
                    case "master":
                        return rpcInvocation_Master;
                    case "direct_worker":
                        return rpcInvocation_DirectWorker;
                    case "remote_worker":
                        return rpcInvocation_RemoteWorker;
                    default:
                        break;
                }
            }
            return 0;
        }
    }
    
    static class NullMeasurer extends OpflowPromMeasurer {
        
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
    
    public static final OpflowPromMeasurer NULL = new NullMeasurer();
}
