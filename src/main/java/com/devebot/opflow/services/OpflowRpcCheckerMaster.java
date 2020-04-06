package com.devebot.opflow.services;

import com.devebot.opflow.OpflowConstant;
import com.devebot.opflow.OpflowRestrictor;
import com.devebot.opflow.OpflowRpcAmqpMaster;
import com.devebot.opflow.OpflowRpcAmqpRequest;
import com.devebot.opflow.OpflowRpcAmqpResult;
import com.devebot.opflow.OpflowRpcChecker;
import com.devebot.opflow.OpflowRpcHttpMaster;
import com.devebot.opflow.OpflowRpcObserver;
import com.devebot.opflow.OpflowRpcParameter;
import com.devebot.opflow.OpflowRpcRoutingInfo;
import com.devebot.opflow.OpflowUUID;
import com.devebot.opflow.OpflowUtil;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowRequestFailureException;
import com.devebot.opflow.exception.OpflowRequestTimeoutException;
import com.devebot.opflow.exception.OpflowRpcMasterDisabledException;
import com.devebot.opflow.exception.OpflowWorkerNotFoundException;
import com.devebot.opflow.supports.OpflowDateTime;
import com.devebot.opflow.supports.OpflowJsonTool;
import java.util.Date;
import java.util.Map;

/**
 *
 * @author acegik
 */
public class OpflowRpcCheckerMaster extends OpflowRpcChecker {

    private final static String DEFAULT_BALL_JSON = OpflowJsonTool.toString(new Object[] { new OpflowRpcChecker.Ping() });

    private final OpflowRestrictor.Valve restrictor;
    private final OpflowRpcAmqpMaster amqpMaster;
    private final OpflowRpcHttpMaster httpMaster;
    private final OpflowRpcObserver rpcObserver;

    private OpflowConstant.Protocol protocol = OpflowConstant.Protocol.AMQP;

    public OpflowRpcCheckerMaster(OpflowRestrictor.Valve restrictor, OpflowRpcObserver rpcObserver, OpflowRpcAmqpMaster amqpMaster, OpflowRpcHttpMaster httpMaster) throws OpflowBootstrapException {
        this.restrictor = restrictor;
        this.rpcObserver = rpcObserver;
        this.amqpMaster = amqpMaster;
        this.httpMaster = httpMaster;
    }

    @Override
    public OpflowRpcChecker.Pong send(final OpflowRpcChecker.Ping ping) throws Throwable {
        if (this.restrictor == null) {
            return _send_safe(ping);
        }
        return this.restrictor.filter(new OpflowRestrictor.Action<OpflowRpcChecker.Pong>() {
            @Override
            public OpflowRpcChecker.Pong process() throws Throwable {
                return _send_safe(ping);
            }
        });
    }

    private OpflowConstant.Protocol next() {
        OpflowConstant.Protocol current = protocol;
        switch (protocol) {
            case AMQP:
                protocol = OpflowConstant.Protocol.HTTP;
                break;
            case HTTP:
                protocol = OpflowConstant.Protocol.AMQP;
                break;
        }
        return current;
    }

    private OpflowRpcChecker.Pong _send_safe(final OpflowRpcChecker.Ping ping) throws Throwable {
        OpflowConstant.Protocol proto = next();
        Date startTime = new Date();

        String body = (ping == null) ? DEFAULT_BALL_JSON : OpflowJsonTool.toString(new Object[] { ping });
        String routineId = OpflowUUID.getBase64ID();
        String routineTimestamp = OpflowDateTime.toISO8601UTC(startTime);
        String routineSignature = OpflowRpcChecker.getSendMethodName();

        OpflowRpcChecker.Pong pong = null;

        switch (proto) {
            case AMQP:
                pong = send_over_amqp(routineId, routineTimestamp, routineSignature, body);
                break;
            case HTTP:
                pong = send_over_http(routineId, routineTimestamp, routineSignature, body);
                break;
            default:
                pong = new OpflowRpcChecker.Pong();
                break;
        }

        Date endTime = new Date();

        // updateInfo the observation result
        if (rpcObserver != null) {
            Map<String, Object> serverletInfo = pong.getAccumulator();
            if (serverletInfo != null) {
                String componentId = serverletInfo.getOrDefault(OpflowConstant.COMPONENT_ID, "").toString();
                if (!componentId.isEmpty()) {
                    if (!rpcObserver.containsInfo(componentId, OpflowConstant.INFO_SECTION_SOURCE_CODE)) {
                        Object serverletCodeRef = serverletInfo.get(OpflowConstant.INFO_SECTION_SOURCE_CODE);
                        if (serverletCodeRef != null) {
                            rpcObserver.updateInfo(componentId, OpflowConstant.INFO_SECTION_SOURCE_CODE, serverletCodeRef);
                        }
                    }
                }
            }
        }
        // append the context of ping
        pong.getParameters().put(OpflowConstant.ROUTINE_ID, routineId);
        pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_PROTOCOL, proto);
        pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_START_TIMESTAMP, startTime);
        pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_END_TIMESTAMP, endTime);
        pong.getParameters().put(OpflowConstant.OPFLOW_COMMON_ELAPSED_TIME, endTime.getTime() - startTime.getTime());
        return pong;
    }

    private OpflowRpcChecker.Pong send_over_amqp(String routineId, String routineTimestamp, String routineSignature, String body) throws Throwable {
        if (amqpMaster == null) {
            throw new OpflowRpcMasterDisabledException("The AMQP Master is disabled");
        }
        try {
            OpflowRpcAmqpRequest rpcRequest = amqpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                    .setProgressEnabled(false)
                    .setRoutineScope("internal"));
            OpflowRpcAmqpResult rpcResult = rpcRequest.extractResult(false);

            if (rpcResult.isTimeout()) {
                throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
            }

            if (rpcResult.isFailed()) {
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcResult.getErrorAsString());
                throw OpflowUtil.rebuildInvokerException(errorMap);
            }

            rpcObserver.setCongestive(OpflowConstant.Protocol.AMQP, false);

            return OpflowJsonTool.toObject(rpcResult.getValueAsString(), OpflowRpcChecker.Pong.class);
        }
        catch (Throwable t) {
            rpcObserver.setCongestive(OpflowConstant.Protocol.AMQP, true);
            throw t;
        }
    }

    private OpflowRpcChecker.Pong send_over_http(String routineId, String routineTimestamp, String routineSignature, String body) throws Throwable {
        if (httpMaster == null) {
            throw new OpflowRpcMasterDisabledException("The HTTP Master is disabled");
        }
        OpflowRpcRoutingInfo routingInfo = rpcObserver.getRoutingInfo(OpflowConstant.Protocol.HTTP, false);
        if (routingInfo == null) {
            rpcObserver.setCongestive(OpflowConstant.Protocol.HTTP, true);
            throw new OpflowWorkerNotFoundException();
        }
        try {
            OpflowRpcHttpMaster.Session rpcRequest = httpMaster.request(routineSignature, body, (new OpflowRpcParameter(routineId, routineTimestamp))
                    .setProgressEnabled(false)
                    .setRoutineScope("internal"), routingInfo);

            if (rpcRequest.isFailed()) {
                Map<String, Object> errorMap = OpflowJsonTool.toObjectMap(rpcRequest.getErrorAsString());
                throw OpflowUtil.rebuildInvokerException(errorMap);
            }

            if (rpcRequest.isCracked()) {
                throw new OpflowRequestFailureException("OpflowRpcChecker.send() call is cracked");
            }

            if (rpcRequest.isTimeout()) {
                throw new OpflowRequestTimeoutException("OpflowRpcChecker.send() call is timeout");
            }

            if (!rpcRequest.isOk()) {
                throw new OpflowRequestFailureException("OpflowRpcChecker.send() is unreasonable");
            }

            rpcObserver.setCongestive(OpflowConstant.Protocol.HTTP, false, routingInfo.getComponentId());
            return OpflowJsonTool.toObject(rpcRequest.getValueAsString(), OpflowRpcChecker.Pong.class);
        }
        catch (Throwable t) {
            rpcObserver.setCongestive(OpflowConstant.Protocol.HTTP, true, routingInfo.getComponentId());
            throw t;
        }
    }
}