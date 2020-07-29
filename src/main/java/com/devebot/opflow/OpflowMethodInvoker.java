package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowTargetRoutine;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowInstantiationException;
import com.devebot.opflow.exception.OpflowJsonSyntaxException;
import com.devebot.opflow.exception.OpflowMethodNotFoundException;
import com.devebot.opflow.exception.OpflowTargetNotFoundException;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowSystemInfo;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author pnhung177
 */
public class OpflowMethodInvoker {
    
    private static final Logger LOG = LoggerFactory.getLogger(OpflowMethodInvoker.class);
    private final OpflowLogTracer logTracer;
    private final OpflowRpcAmqpWorker amqpWorker;
    private final OpflowRpcAmqpWorker.Listener amqpListener;
    private final OpflowRpcHttpWorker httpWorker;
    private final OpflowRpcHttpWorker.Listener httpListener;
    private final OpflowPubsubHandler subscriber;
    private final OpflowPubsubListener subListener;
    private final Set<String> routineSignatures = new HashSet<>();
    private final Map<String, Method> methodRef = new HashMap<>();
    private final Map<String, Object> targetRef = new HashMap<>();
    private final Map<String, String> methodOfAlias = new HashMap<>();
    private volatile boolean processing = false;

    public OpflowMethodInvoker(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber) throws OpflowBootstrapException {
        this(amqpWorker, httpWorker, subscriber, null);
    }

    public OpflowMethodInvoker(OpflowRpcAmqpWorker amqpWorker, OpflowRpcHttpWorker httpWorker, OpflowPubsubHandler subscriber, Map<String, Object> options) throws OpflowBootstrapException {
        if (amqpWorker == null && subscriber == null) {
            throw new OpflowBootstrapException("Both of amqpWorker and subscriber must not be null");
        }
        options = OpflowObjectTree.ensureNonNull(options);
        final String componentId = OpflowUtil.getStringField(options, OpflowConstant.COMPONENT_ID, true);
        this.logTracer = OpflowLogTracer.ROOT.branch("instantiatorId", componentId);

        this.amqpWorker = amqpWorker;
        this.amqpListener = new OpflowRpcAmqpWorker.Listener() {
            @Override
            public Boolean processMessage(final OpflowEngine.Message message, final OpflowRpcAmqpResponse response) throws IOException {
                final Map<String, Object> headers = message.getHeaders();
                final String routineId = response.getRoutineId();
                final String routineTimestamp = response.getRoutineTimestamp();
                final String routineScope = response.getRoutineScope();
                final String routineSignature = response.getRoutineSignature();
                final String body = message.getBodyAsString();

                Map<String, String> extra = OpflowObjectTree.<String>buildMap()
                    .put("replyToQueue", response.getReplyQueueName())
                    .put("consumerTag", response.getConsumerTag())
                    .toMap();

                RoutineOutput output = invokeRoutine(OpflowConstant.Protocol.AMQP, body, routineSignature, routineScope, routineTimestamp, routineId, componentId, extra);
                output.fill(response);

                return null;
            }
        };

        this.httpWorker = httpWorker;
        this.httpListener = new OpflowRpcHttpWorker.Listener() {
            @Override
            public OpflowRpcHttpWorker.Output processMessage(String body, String routineSignature, String routineScope, String routineTimestamp, String routineId, Map<String, String> extra) {
                return invokeRoutine(OpflowConstant.Protocol.HTTP, body, routineSignature, routineScope, routineTimestamp, routineId, componentId, extra).export();
            }
        };

        this.subscriber = subscriber;
        this.subListener = new OpflowPubsubListener() {
            @Override
            public void processMessage(OpflowEngine.Message message) throws IOException {
                final Map<String, Object> headers = message.getHeaders();
                final String routineId = OpflowUtil.getRoutineId(headers);
                final String routineTimestamp = OpflowUtil.getRoutineTimestamp(headers);
                final String routineSignature = OpflowUtil.getRoutineSignature(headers);
                final String methodSignature = methodOfAlias.getOrDefault(routineSignature, routineSignature);
                final OpflowLogTracer reqTracer = logTracer.branch(OpflowConstant.REQUEST_TIME, routineTimestamp)
                    .branch(OpflowConstant.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(headers));
                if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) {
                    LOG.info(reqTracer
                        .put("routineSignature", routineSignature)
                        .put("methodSignature", methodSignature)
                        .text("Request[${requestId}][${requestTime}] - Serverlet[${instantiatorId}] receives an asynchronous routine call to method[${methodSignature}]")
                        .stringify());
                }
                Method method = methodRef.get(methodSignature);
                Object target = targetRef.get(methodSignature);
                assertMethodNotNull(methodSignature, method, target, reqTracer);
                try {
                    Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
                    OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(origin, OpflowTargetRoutine.class);
                    if (routine != null && routine.enabled() == false) {
                        throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
                    }

                    String json = message.getBodyAsString();
                    if (reqTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) {
                        LOG.trace(reqTracer
                            .put("arguments", json)
                            .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                            .stringify());
                    }
                    Object[] args = OpflowJsonTool.toObjectArray(json, method.getParameterTypes());

                    method.invoke(target, args);

                    if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) {
                        LOG.info(reqTracer
                            .text("Request[${requestId}][${requestTime}] - Method call has completed")
                            .stringify());
                    }
                } catch (OpflowJsonSyntaxException error) {
                    throw error;
                } catch (IllegalAccessException | IllegalArgumentException | NoSuchMethodException | SecurityException | UnsupportedOperationException ex) {
                    throw new IOException(ex);
                } catch (InvocationTargetException exception) {
                    Throwable catched = exception.getCause();
                    catched.getStackTrace();
                    throw new IOException(catched);
                }
            }
        };

        if (Boolean.TRUE.equals(options.get(OpflowConstant.OPFLOW_COMMON_AUTORUN))) {
            process();
        }
    }

    private RoutineOutput invokeRoutine(
        final OpflowConstant.Protocol protocol,
        final String body,
        final String routineSignature,
        final String routineScope,
        final String routineTimestamp,
        final String routineId,
        final String componentId,
        final Map<String, String> extra
    ) {
        RoutineOutput output = null;
        final String methodSignature = methodOfAlias.getOrDefault(routineSignature, routineSignature);
        final OpflowLogTracer reqTracer = logTracer.branch(OpflowConstant.REQUEST_TIME, routineTimestamp)
            .branch(OpflowConstant.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));
        if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) {
            LOG.info(reqTracer
                .put("methodSignature", methodSignature)
                .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-received]"
                    + " - Serverlet[${instantiatorId}][${instanceId}] receives a RPC call to the routine[${methodSignature}]")
                .stringify());
        }
        Method method = methodRef.get(methodSignature);
        Object target = targetRef.get(methodSignature);
        assertMethodNotNull(methodSignature, method, target, reqTracer);
        try {
            Method origin = target.getClass().getMethod(method.getName(), method.getParameterTypes());
            OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(origin, OpflowTargetRoutine.class);;
            if (routine != null && routine.enabled() == false) {
                throw new UnsupportedOperationException("Method " + origin.toString() + " is disabled");
            }

            if (reqTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) {
                LOG.trace(reqTracer
                    .put("arguments", body)
                    .text("Request[${requestId}][${requestTime}] - Method arguments in json string")
                    .stringify());
            }
            Object[] args = OpflowJsonTool.toObjectArray(body, method.getGenericParameterTypes());

            Object returnValue;

            String pingSignature = OpflowServerlet.OpflowRpcCheckerWorker.getSendMethodName();
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) {
                LOG.trace(reqTracer
                    .put("routineSignature", routineSignature)
                    .put("pingSignature", pingSignature)
                    .text("Request[${requestId}][${requestTime}] - compares the routine[${routineSignature}] with ping[${pingSignature}]")
                    .stringify());
            }
            if (pingSignature.equals(routineSignature)) {
                if (args.length > 0) {
                    OpflowRpcChecker.Ping p = (OpflowRpcChecker.Ping) args[0];
                    if (p.q != null) {
                        String q = p.q;
                        if (q.equals(getClassNameLabel(IllegalAccessException.class))) {
                            throw new IllegalAccessException();
                        }
                        if (q.equals(getClassNameLabel(IllegalArgumentException.class))) {
                            throw new IllegalArgumentException();
                        }
                        if (q.equals(getClassNameLabel(NoSuchMethodException.class))) {
                            throw new NoSuchMethodException();
                        }
                        if (q.equals(getClassNameLabel(SecurityException.class))) {
                            throw new SecurityException();
                        }
                        if (q.equals(getClassNameLabel(UnsupportedOperationException.class))) {
                            throw new UnsupportedOperationException();
                        }
                        if (q.equals(getClassNameLabel(OpflowJsonSyntaxException.class))) {
                            OpflowJsonTool.toObject("{opflow}", OpflowRpcChecker.Ping.class);
                            throw new Exception();
                        }
                    }
                }
                returnValue = new OpflowRpcChecker.Pong(OpflowObjectTree.buildMap(new OpflowObjectTree.Listener<Object>() {
                    @Override
                    public void transform(Map<String, Object> opts) {
                        Map<String, Object> requestInfo = OpflowObjectTree.buildMap()
                                .put(OpflowConstant.ROUTINE_ID, routineId)
                                .put(OpflowConstant.ROUTINE_TIMESTAMP, routineTimestamp)
                                .add(extra)
                                .toMap();
                        OpflowEngine engine = amqpWorker.getEngine();
                        opts.put(OpflowConstant.COMPONENT_ID, componentId);
                        opts.put(OpflowConstant.COMP_RPC_AMQP_WORKER, OpflowObjectTree.buildMap()
                            .put(OpflowConstant.COMPONENT_ID, amqpWorker.getComponentId())
                            .put(OpflowConstant.AMQP_PARAM_APP_ID, engine.getAppId())
                            .put(OpflowConstant.OPFLOW_INCOMING_QUEUE_NAME, amqpWorker.getIncomingQueueName())
                            .put("request", requestInfo, protocol == OpflowConstant.Protocol.AMQP)
                            .toMap());
                        opts.put(OpflowConstant.COMP_RPC_HTTP_WORKER, OpflowObjectTree.buildMap()
                            .put(OpflowConstant.COMPONENT_ID, httpWorker.getComponentId())
                            .put("request", requestInfo, protocol == OpflowConstant.Protocol.HTTP)
                            .toMap());
                        opts.put(OpflowConstant.INFO_SECTION_SOURCE_CODE, OpflowObjectTree.buildMap()
                            .put("server", OpflowUtil.getGitInfoFromWorker())
                            .put(OpflowConstant.FRAMEWORK_ID, OpflowSystemInfo.getGitInfo())
                            .toMap());
                    }
                }).toMap());
            } else {
                if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) {
                    LOG.info(reqTracer
                        .put("targetName", target.getClass().getName())
                        .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-processing] - The method from target[${targetName}] is invoked")
                        .stringify());
                }
                returnValue = method.invoke(target, args);
            }

            String result = OpflowJsonTool.toString(returnValue);
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) {
                LOG.trace(reqTracer
                    .put("return", OpflowUtil.truncate(result))
                    .text("Request[${requestId}][${requestTime}] - Return the output of the method")
                    .stringify());
            }
            output = RoutineOutput.asSuccess(result);

            if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) {
                LOG.info(reqTracer
                    .text("Request[${requestId}][${requestTime}][x-serverlet-rpc-completed] - Method call has completed")
                    .stringify());
            }
        } catch (OpflowJsonSyntaxException error) {
            error.getStackTrace();
            output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                .put("exceptionClass", error.getClass().getName())
                .put("exceptionPayload", OpflowJsonTool.toString(error))
                .put("type", error.getClass().getName())
                .put("message", error.getMessage())
                .toString());
            // throw error;
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException ex) {
            LOG.error(null, ex); // not expected to happen
            ex.getStackTrace();
            output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                .put("exceptionClass", ex.getClass().getName())
                .put("exceptionPayload", OpflowJsonTool.toString(ex))
                .put("type", ex.getClass().getName())
                .put("message", ex.getMessage())
                .toString());
        } catch (InvocationTargetException ex) {
            Throwable cause = (Exception) ex.getCause();
            if (cause == null) {
                cause = ex;
            }
            cause.getStackTrace();
            output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                .put("exceptionClass", cause.getClass().getName())
                .put("exceptionPayload", OpflowJsonTool.toString(cause))
                .put("type", cause.getClass().getName())
                .put("message", cause.getMessage())
                .toString());
        } catch (UnsupportedOperationException ex) {
            ex.getStackTrace();
            output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                .put("exceptionClass", ex.getClass().getName())
                .put("exceptionPayload", OpflowJsonTool.toString(ex))
                .put("type", ex.getClass().getName())
                .put("message", ex.getMessage())
                .toString());
        } catch (Exception ex) {
            output = RoutineOutput.asFailure(OpflowObjectTree.buildMap(false)
                .put("type", ex.getClass().getName())
                .put("message", ex.getMessage())
                .toString());
        }
        return output;
    }

    private static class RoutineOutput {
        private boolean failed;
        private String value;
        private String error;

        public static RoutineOutput asSuccess(String value) {
            RoutineOutput that = new RoutineOutput();
            that.failed = false;
            that.value = value;
            return that;
        }

        public static RoutineOutput asFailure(String error) {
            RoutineOutput that = new RoutineOutput();
            that.failed = true;
            that.error = error;
            return that;
        }

        public void fill(OpflowRpcAmqpResponse response) {
            if (failed) {
                response.emitFailed(error);
            } else {
                response.emitCompleted(value);
            }
        }

        public OpflowRpcHttpWorker.Output export() {
            if (failed) {
                return new OpflowRpcHttpWorker.Output(false, error);
            } else {
                return new OpflowRpcHttpWorker.Output(true, value);
            }
        }
    }

    public final synchronized void process() {
        if (!processing) {
            if (amqpWorker != null) {
                amqpWorker.process(routineSignatures, amqpListener);
            }
            if (httpWorker != null) {
                httpWorker.process(routineSignatures, httpListener);
            }
            if (subscriber != null) {
                subscriber.subscribe(subListener);
            }
            processing = true;
        }
    }

    public void instantiateType(Class type) {
        instantiateType(type, null);
    }

    public void instantiateType(Class type, Object target) {
        if (type == null) {
            throw new OpflowInstantiationException("The [type] parameter must not be null");
        }
        if (Modifier.isAbstract(type.getModifiers()) && target == null) {
            if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(logTracer
                    .text("Class should not be an abstract type")
                    .stringify());
            }
            throw new OpflowInstantiationException("Class should not be an abstract type");
        }
        try {
            if (target == null) {
                target = type.getDeclaredConstructor().newInstance();
            }
            for (Method method : type.getDeclaredMethods()) {
                String methodSignature = OpflowUtil.getMethodSignature(method);
                OpflowTargetRoutine routine = OpflowUtil.extractMethodAnnotation(method, OpflowTargetRoutine.class);
                if (routine != null && routine.alias() != null) {
                    String[] aliases = routine.alias();
                    if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) {
                        LOG.debug(logTracer
                            .put("methodSignature", methodSignature)
                            .put("numberOfAliases", aliases.length)
                            .text("Serverlet[${instantiatorId}].instantiateType() - method[${methodSignature}] has ${numberOfAliases} alias(es)")
                            .stringify());
                    }
                    for (String alias : aliases) {
                        if (methodOfAlias.containsKey(alias)) {
                            throw new OpflowInstantiationException("Alias[" + alias + "]/methodSignature[" + methodSignature + "]"
                                + " is conflicted with alias of routineSignature[" + methodOfAlias.get(alias) + "]");
                        }
                        methodOfAlias.put(alias, methodSignature);
                        if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) {
                            LOG.debug(logTracer
                                .put("alias", alias)
                                .put("methodSignature", methodSignature)
                                .text("Serverlet[${instantiatorId}].instantiateType() - link the alias[${alias}] to the methodSignature[${methodSignature}]")
                                .stringify());
                        }
                    }
                }
            }
            routineSignatures.addAll(methodOfAlias.keySet());
            List<Class<?>> clazzes = OpflowUtil.getAllAncestorTypes(type);
            for (Class clz : clazzes) {
                Method[] methods = clz.getDeclaredMethods();
                for (Method method : methods) {
                    String methodSignature = OpflowUtil.getMethodSignature(method);
                    if (logTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) {
                        LOG.trace(logTracer
                            .put("amqpWorkerId", amqpWorker.getComponentId())
                            .put("methodSignature", methodSignature)
                            .tags("attach-method-to-RpcWorker-listener")
                            .text("Attach the method[" + methodSignature + "] to the listener of amqpWorker[${amqpWorkerId}]")
                            .stringify());
                    }
                    if (!routineSignatures.add(methodSignature) && !method.equals(methodRef.get(methodSignature))) {
                        throw new OpflowInstantiationException("methodSignature[" + methodSignature + "] is conflicted");
                    }
                    methodRef.put(methodSignature, method);
                    targetRef.put(methodSignature, target);
                }
            }
        } catch (InstantiationException except) {
            if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(logTracer
                    .put("errorType", except.getClass().getName())
                    .put("errorMessage", except.getMessage())
                    .text("Could not instantiate the class")
                    .stringify());
            }
            throw new OpflowInstantiationException("Could not instantiate the class", except);
        } catch (IllegalAccessException except) {
            if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(logTracer
                    .put("errorType", except.getClass().getName())
                    .put("errorMessage", except.getMessage())
                    .text("Constructor is not accessible")
                    .stringify());
            }
            throw new OpflowInstantiationException("Constructor is not accessible", except);
        } catch (SecurityException except) {
            if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(logTracer
                    .put("errorType", except.getClass().getName())
                    .put("errorMessage", except.getMessage())
                    .text("Class loaders is not the same or denies access")
                    .stringify());
            }
            throw new OpflowInstantiationException("Class loaders is not the same or denies access", except);
        } catch (Exception except) {
            if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(logTracer
                    .put("errorType", except.getClass().getName())
                    .put("errorMessage", except.getMessage())
                    .text("Unknown exception")
                    .stringify());
            }
            throw new OpflowInstantiationException("Unknown exception", except);
        }
        process();
    }

    private void assertMethodNotNull(String methodSignature, Method method, Object target, OpflowLogTracer reqTracer) {
        if (method == null) {
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(reqTracer
                    .put("methodSignature", methodSignature)
                    .text("Request[${requestId}][${requestTime}] - method[${methodSignature}] not found")
                    .stringify());
            }
            throw new OpflowMethodNotFoundException();
        }
        if (target == null) {
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) {
                LOG.error(reqTracer
                    .put("methodSignature", methodSignature)
                    .text("Request[${requestId}][${requestTime}] - target[${methodSignature}] not found")
                    .stringify());
            }
            throw new OpflowTargetNotFoundException();
        }
    }

    private static String getClassNameLabel(Class clazz) {
        return "throw-" + OpflowUtil.getClassSimpleName(clazz);
    }
}
