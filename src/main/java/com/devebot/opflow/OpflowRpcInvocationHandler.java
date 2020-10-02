package com.devebot.opflow;

import com.devebot.opflow.annotation.OpflowSourceRoutine;
import com.devebot.opflow.exception.OpflowInstantiationException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import com.devebot.opflow.exception.OpflowWorkerNotFoundException;
import com.devebot.opflow.services.OpflowRestrictorMaster;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cuoi
 */
public class OpflowRpcInvocationHandler implements InvocationHandler {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcInvocationHandler.class);
    
    private final static int FLAG_AMQP = 1;
    private final static int FLAG_HTTP = 2;
    
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final OpflowRestrictorMaster restrictor;
    private final OpflowReqExtractor reqExtractor;
    private final OpflowRpcObserver rpcObserver;

    private final OpflowRpcAmqpMaster amqpMaster;
    private final OpflowRpcHttpMaster httpMaster;
    private final OpflowPubsubHandler publisher;

    private final Class clazz;
    private final Object nativeWorker;
    private final boolean nativeWorkerEnabled;
    private boolean nativeWorkerActive = true;
    private final Map<String, String> aliasOfMethod = new HashMap<>();
    private final Map<String, Boolean> methodIsAsync = new HashMap<>();

    private boolean publisherActive = true;
    private boolean remoteAMQPWorkerActive = true;
    private boolean remoteHTTPWorkerActive = true;

    private final int[] masterFlags;

    public OpflowRpcInvocationHandler(
        OpflowLogTracer logTracer,
        OpflowPromMeasurer measurer,
        OpflowRestrictorMaster restrictor,
        OpflowReqExtractor reqExtractor,
        OpflowRpcObserver rpcObserver,
        OpflowRpcAmqpMaster amqpMaster,
        OpflowRpcHttpMaster httpMaster,
        OpflowPubsubHandler publisher,
        Class clazz,
        Object nativeWorker,
        boolean nativeWorkerEnabled
    ) {
        this.logTracer = logTracer;
        this.measurer = measurer;
        this.restrictor = restrictor;
        this.reqExtractor = reqExtractor;
        this.rpcObserver = rpcObserver;

        this.amqpMaster = amqpMaster;
        this.httpMaster = httpMaster;
        this.publisher = publisher;

        this.masterFlags = new int[] { FLAG_AMQP, FLAG_HTTP };

        this.clazz = clazz;
        this.nativeWorker = nativeWorker;
        this.nativeWorkerEnabled = nativeWorkerEnabled;

        for (Method method : this.clazz.getDeclaredMethods()) {
            String methodSignature = OpflowUtil.getMethodSignature(method);
            OpflowSourceRoutine routine = OpflowUtil.extractMethodAnnotation(method, OpflowSourceRoutine.class);
            if (routine != null && routine.alias() != null && routine.alias().length() > 0) {
                String alias = routine.alias();
                if (aliasOfMethod.containsValue(alias)) {
                    throw new OpflowInstantiationException("Alias[" + alias + "]/methodSignature[" + methodSignature + "] is duplicated");
                }
                aliasOfMethod.put(methodSignature, alias);
                if (logTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) LOG.trace(logTracer
                        .put("alias", alias)
                        .put("methodSignature", methodSignature)
                        .text("link alias to methodSignature")
                        .stringify());
            }
            methodIsAsync.put(methodSignature, (routine != null) && routine.isAsync());
        }
    }

    public Set<String> getMethodNames() {
        return methodIsAsync.keySet();
    }

    public Set<Map<String, Object>> getMethodInfos() {
        Set<Map<String, Object>> infos = new HashSet<>();
        Set<String> methodNames = methodIsAsync.keySet();
        for (String methodName : methodNames) {
            infos.add(OpflowObjectTree.buildMap()
                .put("method", methodName)
                .put("alias", aliasOfMethod.get(methodName))
                .put("async", methodIsAsync.get(methodName))
                .toMap());
        }
        return infos;
    }

    public boolean isPublisherActive() {
        return publisherActive;
    }

    public void setPublisherActive(boolean active) {
        this.publisherActive = active;
    }

    public boolean isPublisherAvailable() {
        return this.publisher != null && this.publisherActive;
    }

    public boolean isNativeWorkerActive() {
        return nativeWorkerActive;
    }

    public void setNativeWorkerActive(boolean active) {
        this.nativeWorkerActive = active;
    }

    public boolean isNativeWorkerAvailable() {
        return this.nativeWorker != null && this.nativeWorkerEnabled && this.nativeWorkerActive;
    }

    public String getNativeWorkerClassName() {
        if (this.nativeWorker == null) return null;
        return this.nativeWorker.getClass().getName();
    }

    public Integer getNativeWorkerHashCode() {
        if (this.nativeWorker == null) return null;
        return this.nativeWorker.hashCode();
    }

    public boolean isRemoteAMQPWorkerActive() {
        return this.remoteAMQPWorkerActive;
    }

    public void setRemoteAMQPWorkerActive(boolean active) {
        this.remoteAMQPWorkerActive = active;
    }

    public boolean isRemoteAMQPWorkerAvailable() {
        return amqpMaster != null && rpcObserver != null && !rpcObserver.isCongestive(OpflowConstant.Protocol.AMQP) && isRemoteAMQPWorkerActive();
    }

    public boolean isRemoteHTTPWorkerActive() {
        return this.remoteHTTPWorkerActive;
    }

    public void setRemoteHTTPWorkerActive(boolean active) {
        this.remoteHTTPWorkerActive = active;
    }

    public boolean isRemoteHTTPWorkerAvailable() {
        return httpMaster != null && rpcObserver != null && !rpcObserver.isCongestive(OpflowConstant.Protocol.HTTP) && isRemoteHTTPWorkerActive();
    }

    @Override
    public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
        if (this.restrictor == null) {
            return _invoke(proxy, method, args);
        }
        return this.restrictor.filter(new OpflowRestrictor.Action<Object>() {
            @Override
            public Object process() throws Throwable {
                return _invoke(proxy, method, args);
            }
        });
    }

    private Object _invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // generate the routineId
        final String routineId = OpflowUUID.getBase64ID();

        // generate the routineTimestamp
        final String routineTimestamp = OpflowDateTime.getCurrentTimeString();

        // create the logTracer
        final OpflowLogTracer reqTracer = logTracer.branch(OpflowConstant.REQUEST_TIME, routineTimestamp).branch(OpflowConstant.REQUEST_ID, routineId);

        // get the method signature
        String methodSignature = OpflowUtil.getMethodSignature(method);

        // convert the method signature to routineSignature
        String routineSignature = aliasOfMethod.getOrDefault(methodSignature, methodSignature);

        // determine the requestId
        final String requestId;
        if (reqExtractor != null) {
            String _requestId = reqExtractor.extractRequestId(args);
            requestId = (_requestId != null) ? _requestId : "REQ:" + routineId;
        } else {
            requestId = null;
        }

        Boolean isAsync = methodIsAsync.getOrDefault(methodSignature, false);
        if (reqTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(reqTracer
                .put("isAsync", isAsync)
                .put("externalRequestId", requestId)
                .put("methodSignature", methodSignature)
                .put("routineSignature", routineSignature)
                .text("Request[${requestId}][${requestTime}][x-commander-invocation-begin]" +
                        " - Commander[${commanderId}][${instanceId}]" +
                        " - method[${routineSignature}] is async [${isAsync}] with requestId[${externalRequestId}]")
                .stringify());

        if (args == null) args = new Object[0];
        String body = OpflowJsonTool.toString(args);

        if (reqTracer.ready(LOG, OpflowLogTracer.Level.TRACE)) LOG.trace(reqTracer
                .put("args", args)
                .put("body", body)
                .text("Request[${requestId}][${requestTime}] - RpcInvocationHandler.invoke() details")
                .stringify());

        if (this.publisher != null && this.publisherActive && isAsync && void.class.equals(method.getReturnType())) {
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                    .text("Request[${requestId}][${requestTime}][x-commander-publish-method] - RpcInvocationHandler.invoke() dispatch the call to the publisher")
                    .stringify());
            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_FLOW_PUBSUB, routineSignature, OpflowConstant.METHOD_INVOCATION_STATUS_ENTER);
            if (this.publisher.isSingleArgumentMode() && args.length == 1) {
                if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-single-argument-mode] - apply single argument")
                        .stringify());
                body = args[0].toString();
            }
            this.publisher.publish(body, OpflowObjectTree.buildMap(false)
                    .put(CONST.AMQP_HEADER_ROUTINE_ID, routineId)
                    .put(CONST.AMQP_HEADER_ROUTINE_TIMESTAMP, routineTimestamp)
                    .put(CONST.AMQP_HEADER_ROUTINE_SIGNATURE, routineSignature)
                    .toMap());
            return null;
        } else {
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                    .text("Request[${requestId}][${requestTime}][x-commander-dispatch-method] - RpcInvocationHandler.invoke() dispatch the call to the RPC Master")
                    .stringify());
            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_FLOW_RPC, routineSignature, OpflowConstant.METHOD_INVOCATION_STATUS_ENTER);
        }

        if (!isNativeWorkerAvailable() && !isRemoteAMQPWorkerActive() && !isRemoteHTTPWorkerActive()) {
            throw new OpflowWorkerNotFoundException("all of workers are deactivated");
        }

        boolean unfinished = false;

        for (int flag : masterFlags) {
            if (flag == FLAG_AMQP) {
                if (isRemoteAMQPWorkerAvailable()) {
                    unfinished = false;

                    OpflowRpcAmqpRequest amqpSession = amqpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                            .setProgressEnabled(false));
                    OpflowRpcAmqpResult amqpResult = amqpSession.extractResult(false);

                    if (amqpResult.isCompleted()) {
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                                .put("returnType", method.getReturnType().getName())
                                .put("returnValue", amqpResult.getValueAsString())
                                .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-ok] - RpcInvocationHandler.invoke() return the output")
                                .stringify());

                        measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "ok");

                        if (method.getReturnType() == void.class) return null;

                        return OpflowJsonTool.toObject(amqpResult.getValueAsString(), method.getReturnType());
                    }

                    if (amqpResult.isFailed()) {
                        measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "failed");
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                                .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-failed] - RpcInvocationHandler.invoke() has failed")
                                .stringify());
                        Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(amqpResult.getErrorAsString());
                        throw OpflowUtil.rebuildInvokerException(errorMap);
                    }

                    if (amqpResult.isTimeout()) {
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                                .text("Request[${requestId}][${requestTime}][x-commander-remote-amqp-worker-timeout] - RpcInvocationHandler.invoke() is timeout")
                                .stringify());
                    }

                    unfinished = true;
                    if (rpcObserver != null) {
                        rpcObserver.setCongestive(OpflowConstant.Protocol.AMQP, true);
                    }
                }
            }

            if (flag == FLAG_HTTP) {
                OpflowRpcRoutingInfo routingInfo = null;
                if (rpcObserver != null) {
                    routingInfo = rpcObserver.getRoutingInfo(OpflowConstant.Protocol.HTTP);
                }
                if (isRemoteHTTPWorkerAvailable() && routingInfo != null) {
                    unfinished = false;

                    OpflowRpcHttpMaster.Session httpSession = httpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                            .setProgressEnabled(false), routingInfo);

                    if (httpSession.isOk()) {
                        measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER, routineSignature, "ok");
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                                .put("returnType", method.getReturnType().getName())
                                .put("returnValue", httpSession.getValueAsString())
                                .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-ok] - RpcInvocationHandler.invoke() return the output")
                                .stringify());
                        if (method.getReturnType() == void.class) return null;
                        return OpflowJsonTool.toObject(httpSession.getValueAsString(), method.getReturnType());
                    }

                    if (httpSession.isFailed()) {
                        measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_HTTP_WORKER, routineSignature, "failed");
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(reqTracer
                                .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-failed] - RpcInvocationHandler.invoke() has failed")
                                .stringify());
                        Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(httpSession.getErrorAsString());
                        throw OpflowUtil.rebuildInvokerException(errorMap);
                    }

                    if (httpSession.isTimeout()) {
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) {
                            LOG.debug(reqTracer
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-timeout] - RpcInvocationHandler.invoke() is timeout")
                                    .stringify());
                        }
                    }

                    if (httpSession.isCracked()) {
                        if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) {
                            LOG.debug(reqTracer
                                    .text("Request[${requestId}][${requestTime}][x-commander-remote-http-worker-cracked] - RpcInvocationHandler.invoke() is cracked")
                                    .stringify());
                        }
                    }

                    unfinished = true;
                    if (rpcObserver != null) {
                        rpcObserver.setCongestive(OpflowConstant.Protocol.HTTP, true, routingInfo.getComponentId());
                    }
                }
            }
        }

        if (isNativeWorkerAvailable()) {
            if (unfinished) {
                if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-native-worker-rescue] - RpcInvocationHandler.invoke() rescues by the nativeWorker")
                        .stringify());
                measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER, routineSignature, OpflowConstant.METHOD_INVOCATION_STATUS_RESCUE);
            } else {
                if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.trace(reqTracer
                        .text("Request[${requestId}][${requestTime}][x-commander-native-worker-retain] - RpcInvocationHandler.invoke() retains the nativeWorker")
                        .stringify());
                measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_NATIVE_WORKER, routineSignature, OpflowConstant.METHOD_INVOCATION_STATUS_NORMAL);
            }
            return method.invoke(this.nativeWorker, args);
        } else {
            if (reqTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.trace(reqTracer
                    .text("Request[${requestId}][${requestTime}][x-commander-remote-all-workers-timeout] - RpcInvocationHandler.invoke() is timeout")
                    .stringify());
            measurer.countRpcInvocation(OpflowConstant.COMP_COMMANDER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "timeout");
            throw new OpflowRequestTimeoutException();
        }
    }
}
