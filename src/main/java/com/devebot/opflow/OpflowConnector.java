package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInstantiationException;
import com.devebot.opflow.exception.OpflowRpcRegistrationException;
import com.devebot.opflow.services.OpflowRestrictorMaster;
import com.devebot.opflow.services.OpflowRpcCheckerMaster;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cuoi
 */
public class OpflowConnector {
    public final static String DEFAULT_CONNECTOR_NAME = "_default_";
    private final static Logger LOG = LoggerFactory.getLogger(OpflowConnector.class);

    private final String componentId;
    private final OpflowLogTracer logTracer;
    
    private final boolean strictMode;
    private final OpflowPromMeasurer measurer;
    private final OpflowReqExtractor reqExtractor;
    private final OpflowRestrictorMaster restrictor;
    private final OpflowRpcObserver rpcObserver;
    private final OpflowThroughput.Tuple speedMeter;

    private OpflowRpcChecker rpcChecker;
    private OpflowPubsubHandler publisher;
    private OpflowRpcAmqpMaster amqpMaster;
    private OpflowRpcHttpMaster httpMaster;
    private boolean nativeWorkerEnabled;
    
    public OpflowConnector(Map<String, Object> kwargs) throws OpflowBootstrapException {
        componentId = OpflowUtil.getStringField(kwargs, OpflowConstant.COMPONENT_ID, true);
        logTracer = OpflowLogTracer.ROOT.branch("componentId", componentId);

        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Container[${componentId}][${instanceId}].new()")
                .stringify());

        strictMode = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_STRICT, Boolean.FALSE);
        measurer = OpflowPromMeasurer.getInstance(OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PROM_EXPORTER));
        OpflowRpcInvocationCounter counter = measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER);

        reqExtractor = (OpflowReqExtractor)OpflowUtil.getOptionField(kwargs, OpflowConstant.COMP_REQ_EXTRACTOR, null);
        restrictor = (OpflowRestrictorMaster)OpflowUtil.getOptionField(kwargs, OpflowConstant.COMP_RESTRICTOR, null);
        rpcObserver = (OpflowRpcObserver)OpflowUtil.getOptionField(kwargs, OpflowConstant.COMP_RPC_OBSERVER, null);
        speedMeter = (OpflowThroughput.Tuple)OpflowUtil.getOptionField(kwargs, OpflowConstant.COMP_SPEED_METER, null);

        nativeWorkerEnabled = OpflowUtil.getBooleanField(kwargs, OpflowConstant.PARAM_NATIVE_WORKER_ENABLED, Boolean.TRUE);
        
        if (nativeWorkerEnabled) {
            counter.setNativeWorkerEnabled(true);
            if (speedMeter != null) {
                speedMeter.register(OpflowRpcInvocationCounter.LABEL_RPC_DIRECT_WORKER, counter.getNativeWorkerInfoSource());
            }
        }
        
        Map<String, Object> amqpMasterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_AMQP_MASTER, OpflowConstant.COMP_CFG_AMQP_MASTER);
        Map<String, Object> httpMasterCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_RPC_HTTP_MASTER);
        Map<String, Object> publisherCfg = OpflowUtil.getChildMap(kwargs, OpflowConstant.COMP_PUBLISHER);

        HashSet<String> checkExchange = new HashSet<>();

        if (OpflowUtil.isComponentEnabled(amqpMasterCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(amqpMasterCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY)) {
                throw new OpflowBootstrapException("Invalid RpcMaster connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(amqpMasterCfg, OpflowConstant.OPFLOW_DISPATCH_EXCHANGE_NAME, OpflowConstant.OPFLOW_DISPATCH_ROUTING_KEY))) {
                throw new OpflowBootstrapException("Duplicated RpcMaster connection parameters");
            }
        }

        if (OpflowUtil.isComponentEnabled(publisherCfg)) {
            if (OpflowUtil.isAMQPEntrypointNull(publisherCfg)) {
                throw new OpflowBootstrapException("Invalid Publisher connection parameters");
            }
            if (!checkExchange.add(OpflowUtil.getAMQPEntrypointCode(publisherCfg))) {
                throw new OpflowBootstrapException("Duplicated Publisher connection parameters");
            }
        }

        if (OpflowUtil.isComponentEnabled(amqpMasterCfg)) {
            amqpMaster = new OpflowRpcAmqpMaster(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(OpflowConstant.COMPONENT_ID, componentId);
                    opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    opts.put(OpflowConstant.COMP_RPC_OBSERVER, rpcObserver);
                }
            }, amqpMasterCfg).toMap());
            counter.setRemoteAMQPWorkerEnabled(true);
            if (speedMeter != null) {
                speedMeter.register(OpflowRpcInvocationCounter.LABEL_RPC_REMOTE_AMQP_WORKER, counter.getRemoteAMQPWorkerInfoSource());
            }
        }
        if (OpflowUtil.isComponentEnabled(httpMasterCfg)) {
            httpMaster = new OpflowRpcHttpMaster(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(OpflowConstant.COMPONENT_ID, componentId);
                    opts.put(OpflowConstant.COMP_MEASURER, measurer);
                    opts.put(OpflowConstant.COMP_RPC_OBSERVER, rpcObserver);
                }
            }, httpMasterCfg).toMap());
            counter.setRemoteHTTPWorkerEnabled(true);
            if (speedMeter != null) {
                speedMeter.register(OpflowRpcInvocationCounter.LABEL_RPC_REMOTE_HTTP_WORKER, counter.getRemoteHTTPWorkerInfoSource());
            }
        }
        if (OpflowUtil.isComponentEnabled(publisherCfg)) {
            publisher = new OpflowPubsubHandler(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                @Override
                public void transform(Map<String, Object> opts) {
                    opts.put(OpflowConstant.COMPONENT_ID, componentId);
                    opts.put(OpflowConstant.COMP_MEASURER, measurer);
                }
            }, publisherCfg).toMap());
            counter.setPublisherEnabled(true);
            if (speedMeter != null) {
                speedMeter.register(OpflowRpcInvocationCounter.LABEL_RPC_PUBLISHER, counter.getPublisherInfoSource());
            }
        }
        
        // create a RpcChecker instance after the amqpMaster, httpMaster has already created
        rpcChecker = new OpflowRpcCheckerMaster(restrictor.getValveRestrictor(), rpcObserver, amqpMaster, httpMaster);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("Container[${componentId}][${instanceId}].new() end!")
                .stringify());
    }

    public OpflowPromMeasurer getMeasurer() {
        return measurer;
    }

    public OpflowThroughput.Tuple getSpeedMeter() {
        return speedMeter;
    }

    public OpflowRpcChecker getRpcChecker() {
        return rpcChecker;
    }

    public OpflowPubsubHandler getPublisher() {
        return publisher;
    }

    public OpflowRpcAmqpMaster getAmqpMaster() {
        return amqpMaster;
    }

    public OpflowRpcHttpMaster getHttpMaster() {
        return httpMaster;
    }

    public Map<String, OpflowRpcInvocationHandler> getMappings() {
        return handlers;
    }
    
    public synchronized void serve() {
    }
    
    public synchronized void close() {
        if (publisher != null) publisher.close();
        if (amqpMaster != null) amqpMaster.close();
        if (httpMaster != null) httpMaster.close();
    }

    public void ping(String query) throws Throwable {
        rpcChecker.send(new OpflowRpcChecker.Ping(query));
    }
    
    private final Map<String, OpflowRpcInvocationHandler> handlers = new LinkedHashMap<>();

    private <T> OpflowRpcInvocationHandler getInvocationHandler(Class<T> clazz, T bean) {
        validateType(clazz);
        String clazzName = clazz.getName();
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                .put("className", clazzName)
                .text("getInvocationHandler() get InvocationHandler by type")
                .stringify());
        if (!handlers.containsKey(clazzName)) {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", clazzName)
                    .text("getInvocationHandler() InvocationHandler not found, create new one")
                    .stringify());
            handlers.put(clazzName, new OpflowRpcInvocationHandler(logTracer, measurer, restrictor, reqExtractor, rpcObserver, 
                    amqpMaster, httpMaster, publisher, clazz, bean, nativeWorkerEnabled));
        } else {
            if (strictMode) {
                throw new OpflowRpcRegistrationException("Class [" + clazzName + "] has already registered");
            }
        }
        return handlers.get(clazzName);
    }

    private void removeInvocationHandler(Class clazz) {
        if (clazz == null) return;
        String clazzName = clazz.getName();
        handlers.remove(clazzName);
    }

    private boolean validateType(Class type) {
        boolean ok = true;
        if (OpflowUtil.isGenericDeclaration(type.toGenericString())) {
            ok = false;
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("typeString", type.toGenericString())
                    .text("generic types are unsupported")
                    .stringify());
        }
        Method[] methods = type.getDeclaredMethods();
        for(Method method:methods) {
            if (OpflowUtil.isGenericDeclaration(method.toGenericString())) {
                ok = false;
                if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                        .put("methodString", method.toGenericString())
                        .text("generic methods are unsupported")
                        .stringify());
            }
        }
        if (!ok) {
            throw new OpflowInstantiationException("Generic type/method is unsupported");
        }
        return ok;
    }

    public <T> T registerType(Class<T> type) {
        return registerType(type, null);
    }

    public <T> T registerType(Class<T> type, T bean) {
        if (type == null) {
            throw new OpflowInstantiationException("The [type] parameter must not be null");
        }
        if (OpflowRpcChecker.class.equals(type)) {
            throw new OpflowInstantiationException("Can not register the OpflowRpcChecker type");
        }
        try {
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", type.getName())
                    .put("classLoaderName", type.getClassLoader().getClass().getName())
                    .text("registerType() calls newProxyInstance()")
                    .stringify());
            T t = (T) Proxy.newProxyInstance(type.getClassLoader(), new Class[] {type}, getInvocationHandler(type, bean));
            if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                    .put("className", type.getName())
                    .text("newProxyInstance() has completed")
                    .stringify());
            return t;
        } catch (IllegalArgumentException exception) {
            if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionMessage", exception.getMessage())
                    .text("newProxyInstance() has failed")
                    .stringify());
            throw new OpflowInstantiationException(exception);
        }
    }

    public <T> void unregisterType(Class<T> type) {
        removeInvocationHandler(type);
    }

    public Map<String, Object> getRpcInvocationCounter() {
        return measurer.getRpcInvocationCounter(OpflowConstant.COMP_COMMANDER).toMap();
    }

    public void resetRpcInvocationCounter() {
        measurer.resetRpcInvocationCounter();
    }
    
    public static Map<String, Boolean> applyConnectors(Map<String, OpflowConnector> connectors, final Function<OpflowConnector, Boolean> action) {
        return connectors.entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> action.apply(e.getValue())
        ));
    }
}
