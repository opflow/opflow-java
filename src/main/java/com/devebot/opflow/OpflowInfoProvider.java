package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowRpcChecker;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.handlers.PathTemplateHandler;
import io.undertow.util.Headers;
import java.util.Deque;
import java.util.Map;

/**
 *
 * @author acegik
 */
public class OpflowInfoProvider implements AutoCloseable {

    private final String instanceId;
    private final OpflowRpcMaster rpcMaster;
    private final OpflowRpcChecker rpcChecker;
    private final Undertow server;

    OpflowInfoProvider(OpflowRpcMaster _rpcMaster,
            OpflowRpcChecker _rpcChecker,
            Map<String, Object> kwargs) throws OpflowBootstrapException {
        this(_rpcMaster, _rpcChecker, kwargs, null);
    }
    
    OpflowInfoProvider(OpflowRpcMaster _rpcMaster,
            OpflowRpcChecker _rpcChecker,
            Map<String, Object> kwargs,
            Map<String, HttpHandler> httpHandlers) throws OpflowBootstrapException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);

        instanceId = OpflowUtil.getOptionField(kwargs, "instanceId", true);
        rpcMaster = _rpcMaster;
        rpcChecker = _rpcChecker;

        PathTemplateHandler ptHandler = Handlers.pathTemplate()
                .add("/ping", new PingHandler());

        if (httpHandlers != null) {
            for(Map.Entry<String, HttpHandler> entry:httpHandlers.entrySet()) {
                ptHandler.add(entry.getKey(), entry.getValue());
            }
        }

        server = Undertow.builder()
                .addHttpListener(9999, "0.0.0.0")
                .setHandler(ptHandler)
                .build();
    }

    public OpflowRpcChecker.Info ping() {
        Map<String, Object> me = OpflowUtil.buildOrderedMap(new OpflowUtil.MapListener() {
            @Override
            public void transform(Map<String, Object> opts) {
                OpflowEngine engine = rpcMaster.getEngine();
                opts.put("instanceId", instanceId);
                opts.put("rpcMaster", OpflowUtil.buildOrderedMap()
                        .put("instanceId", rpcMaster.getInstanceId())
                        .put("exchangeName", engine.getExchangeName())
                        .put("exchangeDurable", engine.getExchangeDurable())
                        .put("routingKey", engine.getRoutingKey())
                        .put("otherKeys", engine.getOtherKeys())
                        .put("applicationId", engine.getApplicationId())
                        .put("callbackQueue", rpcMaster.getCallbackName())
                        .put("callbackDurable", rpcMaster.getCallbackDurable())
                        .put("callbackExclusive", rpcMaster.getCallbackExclusive())
                        .put("callbackAutoDelete", rpcMaster.getCallbackAutoDelete())
                        .toMap());
                opts.put("request", OpflowUtil.buildOrderedMap()
                        .put("expiration", rpcMaster.getExpiration())
                        .toMap());
            }
        }).toMap();
        try {
            return new OpflowRpcChecker.Info(me, this.rpcChecker.send(new OpflowRpcChecker.Ping()));
        } catch (Throwable exception) {
            return new OpflowRpcChecker.Info(me, exception);
        }
    }
    
    public void serve() {
        if (server != null) {
            server.start();
        }
    }
    
    @Override
    public void close() {
        if (server != null) {
            server.stop();
        }
    }
    
    class PingHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            // pretty printing or not?
            boolean pretty = false;
            Deque<String> prettyVals = exchange.getQueryParameters().get("pretty");
            if (prettyVals != null && !prettyVals.isEmpty()) {
                pretty = true;
            }
            try {
                OpflowRpcChecker.Info result = ping();
                if (!"ok".equals(result.getStatus())) {
                    exchange.setStatusCode(503);
                }
                // render the result
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(result.toString(pretty));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
    }
}
