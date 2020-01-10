package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.rabbitmq.client.ConnectionFactory;
import java.util.Date;
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
    
    public abstract RpcInvocationCounter getRpcInvocationCounter(String moduleName);
    
    private Date startTime = new Date();
    
    public Date getStartTime() {
        return startTime;
    }

    private static PipeMeasurer instance = new PipeMeasurer();

    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return instance;
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (OpflowUtil.isComponentEnabled(kwargs)) {
            if (!instance.hasShadow()) {
                instance.setShadow(new OpflowPromExporter(kwargs));
            }
        }
        return instance;
    }
    
    static class RpcInvocationCounter {
        public long total = 0;
        public long directWorker = 0;
        public long remoteWorker = 0;
        
        public RpcInvocationCounter() {
            this(null);
        }
        
        public RpcInvocationCounter(RpcInvocationCounter counter) {
            if (counter != null) {
                this.total = counter.total;
                this.directWorker = counter.directWorker;
                this.remoteWorker = counter.remoteWorker;
            }
        }
    }
    
    static class PipeMeasurer extends OpflowPromMeasurer {

        private OpflowPromMeasurer shadow = null;
        private final RpcInvocationCounter counter = new RpcInvocationCounter();

        public PipeMeasurer() {
        }

        public PipeMeasurer(OpflowPromMeasurer shadow) {
            this.shadow = shadow;
        }

        public boolean hasShadow() {
            return this.shadow != null;
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
            switch (moduleName) {
                case "commander": {
                    synchronized(counter) {
                        switch (eventName) {
                            case "direct_worker":
                                counter.total++;
                                counter.directWorker++;
                                break;
                            case "remote_worker":
                                counter.total++;
                                counter.remoteWorker++;
                                break;
                            default:
                                break;
                        }
                    }
                    break;
                }
            }
        }
        
        @Override
        public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            synchronized(counter) {
                return new RpcInvocationCounter(counter);
            }
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
        public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            return null;
        }
    }
    
    public static final OpflowPromMeasurer NULL = new NullMeasurer();
}
