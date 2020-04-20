package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowOperationException;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public abstract class OpflowPromMeasurer {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPromMeasurer.class);
    private final static OpflowLogTracer LOG_TRACER = OpflowLogTracer.ROOT.copy();
    
    public static enum GaugeAction { INC, DEC }
    
    public abstract void updateComponentInstance(String componentType, String componentId, GaugeAction action);
    
    public abstract void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action);
    
    public abstract void countRpcInvocation(String componentType, String eventName, String routineSignature, String status);
    
    public abstract OpflowRpcInvocationCounter getRpcInvocationCounter(String componentType);
    
    public abstract Map<String, Object> resetRpcInvocationCounter();
    
    public abstract Map<String, Object> getServiceInfo();
    
    public static Class<? extends OpflowPromMeasurer> PromExporter;
    
    private static PipeMeasurer instance = new PipeMeasurer();
    
    public static OpflowPromMeasurer getInstance() throws OpflowOperationException {
        return instance;
    }
    
    public static OpflowPromMeasurer getInstance(Map<String, Object> kwargs) throws OpflowOperationException {
        if (OpflowUtil.isComponentEnabled(kwargs)) {
            if (!instance.hasShadow()) {
                synchronized (OpflowPromMeasurer.class) {
                    if (!instance.hasShadow()) {
                        if (PromExporter != null) {
                            try {
                                instance.setShadow((OpflowPromMeasurer) PromExporter.getDeclaredConstructor(Map.class).newInstance(kwargs));
                                if (LOG_TRACER.ready(LOG, Level.DEBUG)) LOG.debug(LOG_TRACER
                                        .put("className", PromExporter.getName())
                                        .text("Measurer[${instanceId}].getInstance() - create an object [${className}] and assign it to the measurer")
                                        .stringify());
                            }
                            catch (NoSuchMethodException | SecurityException ex) {
                                if (LOG_TRACER.ready(LOG, Level.ERROR)) LOG.error(LOG_TRACER
                                        .put("className", ex.getClass().getName())
                                        .put("message", ex.getMessage())
                                        .text("Measurer[${instanceId}].getInstance() - getDeclaredConstructor() - exception[${className}]: ${message}")
                                        .stringify());
                            }
                            catch (IllegalAccessException | IllegalArgumentException | InstantiationException | InvocationTargetException ex) {
                                if (LOG_TRACER.ready(LOG, Level.ERROR)) LOG.error(LOG_TRACER
                                        .put("className", ex.getClass().getName())
                                        .put("message", ex.getMessage())
                                        .text("Measurer[${instanceId}].getInstance() - newInstance() - exception[${className}]: ${message}")
                                        .stringify());
                            }
                        }
                    }
                }
            }
        }
        return instance;
    }
    
    static class PipeMeasurer extends OpflowPromMeasurer {

        private OpflowPromMeasurer shadow = null;
        private final OpflowRpcInvocationCounter counter = new OpflowRpcInvocationCounter();

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
        public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
            if (shadow != null) {
                shadow.updateComponentInstance(componentType, componentId, action);
            }
        }

        @Override
        public void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action) {
            if (shadow != null) {
                shadow.updateEngineConnection(connectionOwner, connectionType, action);
            }
        }

        @Override
        public void countRpcInvocation(String componentType, String eventName, String routineSignature, String status) {
            if (shadow != null) {
                shadow.countRpcInvocation(componentType, eventName, routineSignature, status);
            }
            if (OpflowConstant.COMP_COMMANDER.equals(componentType)) {
                switch (eventName) {
                    case OpflowConstant.METHOD_INVOCATION_FLOW_RESTRICTOR:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_CANCELLATION:
                                counter.incCancellationRpc();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_SERVICE_NOT_READY:
                                counter.incServiceNotReadyRpc();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_PAUSING_TIMEOUT:
                                counter.incPausingTimeoutRpc();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_SEMAPHORE_TIMEOUT:
                                counter.incSemaphoreTimeoutRpc();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_REJECTED:
                                counter.incRejectedRpc();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_FLOW_PUBSUB:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_ENTER:
                                counter.incPublishingOk();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER:
                        switch (status) {
                            case OpflowConstant.METHOD_INVOCATION_STATUS_RESCUE:
                                counter.incDirectRescue();
                                break;
                            case OpflowConstant.METHOD_INVOCATION_STATUS_NORMAL:
                                counter.incDirectRetain();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER:
                        switch (status) {
                            case "ok":
                                counter.incRemoteAMQPSuccess();
                                break;
                            case "failed":
                                counter.incRemoteAMQPFailure();
                                break;
                            case "timeout":
                                counter.incRemoteAMQPTimeout();
                                break;
                        }
                        break;
                    case OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER:
                        switch (status) {
                            case "ok":
                                counter.incRemoteHTTPSuccess();
                                break;
                            case "failed":
                                counter.incRemoteHTTPFailure();
                                break;
                            case "timeout":
                                counter.incRemoteHTTPTimeout();
                                break;
                        }
                        break;
                    default:
                        break;
                }
            }
        }

        @Override
        public OpflowRpcInvocationCounter getRpcInvocationCounter(String componentType) {
            return counter;
        }

        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            counter.reset();
            return counter.toMap();
        }

        @Override
        public Map<String, Object> getServiceInfo() {
            if (shadow != null) {
                return shadow.getServiceInfo();
            }
            return null;
        }
    }
    
    static class NullMeasurer extends OpflowPromMeasurer {

        @Override
        public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
        }

        @Override
        public void updateEngineConnection(String connectionOwner, String connectionType, GaugeAction action) {
        }

        @Override
        public void countRpcInvocation(String moduleName, String eventName, String routineSignature, String status) {
        }

        @Override
        public OpflowRpcInvocationCounter getRpcInvocationCounter(String moduleName) {
            return null;
        }

        @Override
        public Map<String, Object> resetRpcInvocationCounter() {
            return null;
        }

        @Override
        public Map<String, Object> getServiceInfo() {
            return null;
        }
    }
    
    public static final OpflowPromMeasurer NULL = new NullMeasurer();
}
