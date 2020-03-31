package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowStringUtil;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.util.HeaderMap;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowRpcHttpWorker {
    private final static OpflowConstant CONST = OpflowConstant.CURRENT();
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRpcHttpWorker.class);
    private final OpflowLogTracer logTracer;
    private final OpflowPromMeasurer measurer;
    private final String componentId;
    private final List<Middleware> middlewares = new LinkedList<>();
    private final RoutineHandler routineHandler;
    private final RoutingHandler defaultHandlers;
    private final String host;
    private final String hostname;
    private final Integer port;
    private final long shutdownTimeout;
    private final Thread shutdownHook;
    private Undertow server;
    private GracefulShutdownHandler shutdownHandler;

    public OpflowRpcHttpWorker(Map<String, Object> kwargs) throws OpflowBootstrapException {
        componentId = OpflowUtil.getStringField(kwargs, CONST.COMPONENT_ID, true);
        measurer = (OpflowPromMeasurer) OpflowUtil.getOptionField(kwargs, OpflowConstant.COMP_MEASURER, OpflowPromMeasurer.NULL);
        
        logTracer = OpflowLogTracer.ROOT.branch("httpWorkerId", componentId);
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("httpWorker[${httpWorkerId}][${instanceId}].new()")
                .stringify());
        
        host = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_HOST, "0.0.0.0");
        hostname = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_HOSTNAME, false, false);
        port = OpflowUtil.detectFreePort(kwargs, OpflowConstant.OPFLOW_COMMON_PORTS, new Integer[] {
                8765, 8766, 8767, 8768, 8769, 8770, 8771, 8772, 8773, 8774, 8775, 8776, 8777
        });
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("host", host)
                .put("hostname", hostname)
                .put("port", port)
                .text("httpWorker[${httpWorkerId}][${instanceId}] - open the HTTP server on [${host}:${port}] with hostname[${hostname}]")
                .stringify());
        
        shutdownTimeout = OpflowObjectTree.getOptionValue(kwargs, "shutdownTimeout", Long.class, 1000l);
        
        shutdownHook = new Thread() {
            @Override
            public void run() {
                close();
            }
        };
        
        routineHandler = new RoutineHandler(componentId);
        
        defaultHandlers = new RoutingHandler()
            .post("/routine", new BlockingHandler(routineHandler))
            .setFallbackHandler(new PageNotFoundHandler());
        
        if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .text("httpWorker[${httpWorkerId}][${instanceId}].new() end!")
                .stringify());
    }

    public String getComponentId() {
        return componentId;
    }
    
    public String getHost() {
        return host;
    }
    
    public String getHostname() {
        return hostname;
    }
    
    public Integer getPort() {
        return port;
    }
    
    public String getAddress() {
        if (hostname != null) {
            return hostname + ":" + String.valueOf(port);
        }
        return null;
    }
    
    public Reporter process(final Listener listener) {
        return process(TRUE, listener);
    }
    
    public Reporter process(final String routineSignature, final Listener listener) {
        return process(new Matcher() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignature != null && routineSignature.equals(originRoutineSignature);
            }
        }, listener);
    }
    
    public Reporter process(final Set<String> routineSignatures, final Listener listener) {
        return process(new Matcher() {
            @Override
            public boolean match(String originRoutineSignature) {
                return routineSignatures != null && routineSignatures.contains(originRoutineSignature);
            }
        }, listener);
    }
    
    public Reporter process(Matcher matcher, final Listener listener) {
        if (matcher != null && listener != null) {
            middlewares.add(new Middleware(matcher, listener));
        }
        return new Reporter() {};
    }
    
    public void serve() {
        assertSystemShutdownHook();

        synchronized (this) {
            if (server == null) {
                shutdownHandler = new GracefulShutdownHandler(defaultHandlers);
                
                server = Undertow.builder()
                        .addHttpListener(port, host)
                        .setHandler(shutdownHandler)
                        .build();

                if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                        .text("httpWorker[${httpWorkerId}].serve() a new HTTP server is created")
                        .stringify());
            }
            if (logTracer.ready(LOG, OpflowLogTracer.Level.INFO)) LOG.info(logTracer
                .put("port", port)
                .put("host", host)
                .text("httpWorker[${httpWorkerId}].serve() Server listening on (http://${host}:${port})")
                .stringify());
            server.start();
        }
    }
    
    public synchronized void close() {
        if (server != null) {
            if (shutdownHandler != null) {
                shutdownHandler.shutdown();
                try {
                    if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(logTracer
                            .text("httpWorker[${httpWorkerId}].close() shutdownHandler.awaitShutdown() starting")
                            .stringify());
                    if (shutdownHandler.awaitShutdown(shutdownTimeout)) {
                        if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(logTracer
                                .text("httpWorker[${httpWorkerId}].close() shutdownHandler.awaitShutdown() has done")
                                .stringify());
                    } else {
                        if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(logTracer
                                .text("httpWorker[${httpWorkerId}].close() shutdownHandler.awaitShutdown() is timeout")
                                .stringify());
                    }
                }
                catch (InterruptedException ex) {
                    if (logTracer.ready(LOG, OpflowLogTracer.Level.ERROR)) LOG.error(logTracer
                            .text("httpWorker[${httpWorkerId}].close() shutdownHandler.awaitShutdown() interrupted")
                            .stringify());
                    shutdownHandler.shutdown();
                }
                finally {
                    if (logTracer.ready(LOG, OpflowLogTracer.Level.DEBUG)) LOG.debug(logTracer
                            .text("httpWorker[${httpWorkerId}].close() shutdownHandler.awaitShutdown() finished")
                            .stringify());
                }
                shutdownHandler = null;
            }
            server.stop();
            server = null;
        }
    }
    
    public class Middleware {
        private final Matcher matcher;
        private final Listener listener;

        public Middleware(Matcher matcher, Listener listener) {
            this.matcher = matcher;
            this.listener = listener;
        }

        public Matcher getMatcher() {
            return matcher;
        }

        public Listener getListener() {
            return listener;
        }
    }
    
    public interface Matcher {
        public boolean match(String routineSignature);
    }
    
    private final Matcher TRUE = new Matcher() {
        @Override
        public boolean match(String routineSignature) {
            return true;
        }
    };
    
    public interface Listener {
        Output processMessage(String body, String routineSignature, String routineScope, String routineTimestamp, String routineId, Map<String, String> extra);
    }
    
    public interface Reporter {
    }
    
    public static class Output {
        private final boolean failed;
        private final String value;
        private final String error;
        
        public Output(boolean ok, String text) {
            this.failed = !ok;
            if (failed) {
                this.error = text;
                this.value = null;
            } else {
                this.error = null;
                this.value = text;
            }
        }
        
        public boolean hasError() {
            return failed;
        }
        
        public String getValue() {
            return value;
        }
        
        public String getError() {
            return error;
        }
    }
    
    class RoutineHandler implements HttpHandler {
        final String componentId;
        
        public RoutineHandler(String componentId) {
            this.componentId = componentId;
        }
        
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                // get the HTTP headers
                HeaderMap reqHeaders = exchange.getRequestHeaders();
                
                String routineId = reqHeaders.getFirst(OpflowConstant.HTTP_HEADER_ROUTINE_ID);
                String routineTimestamp = reqHeaders.getFirst(OpflowConstant.HTTP_HEADER_ROUTINE_TIMESTAMP);
                String routineSignature = reqHeaders.getFirst(OpflowConstant.HTTP_HEADER_ROUTINE_SIGNATURE);
                String routineScope = reqHeaders.getFirst(OpflowConstant.HTTP_HEADER_ROUTINE_SCOPE);
                String[] routineTags = OpflowStringUtil.splitByComma(reqHeaders.getFirst(OpflowConstant.HTTP_HEADER_ROUTINE_TAGS));
                
                OpflowLogTracer reqTracer = null;
                if (logTracer.ready(LOG, Level.INFO)) {
                    reqTracer = logTracer.branch(CONST.REQUEST_TIME, routineTimestamp)
                            .branch(CONST.REQUEST_ID, routineId, new OpflowUtil.OmitInternalOplogs(routineScope));
                }
                
                if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                        .put("routineSignature", routineSignature)
                        .text("Request[${requestId}][${requestTime}][x-rpc-http-worker-request-received] - httpWorker[${httpWorkerId}] receives a new RPC [${routineSignature}]")
                        .stringify());
                
                // get the body
                String body = OpflowStringUtil.fromInputStream(exchange.getInputStream());
                
                // processing
                Output output = null;
                int count = 0;
                for(Middleware middleware : middlewares) {
                    if (middleware.getMatcher().match(routineSignature)) {
                        count++;
                        measurer.countRpcInvocation(OpflowConstant.COMP_RPC_HTTP_WORKER, OpflowConstant.METHOD_INVOCATION_REMOTE_AMQP_WORKER, routineSignature, "process");
                        output = middleware.getListener().processMessage(body, routineSignature, routineScope, routineTimestamp, routineId, null);
                        break;
                    }
                }
                
                // rendering the output
                exchange.getResponseHeaders().put(new HttpString(OpflowConstant.OPFLOW_RES_HEADER_SERVERLET_ID), componentId)
                        .put(new HttpString(OpflowConstant.OPFLOW_RES_HEADER_PROTO_VERSION), CONST.OPFLOW_PROTOCOL_VERSION);
                
                if (output != null) {
                    if (reqTracer != null && reqTracer.ready(LOG, Level.INFO)) LOG.info(reqTracer
                            .text("Request[${requestId}][${requestTime}][x-rpc-http-worker-request-finished] - RPC request processing has completed")
                            .stringify());
                    if (output.hasError()) {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        exchange.setStatusCode(500).getResponseSender().send(output.getError());
                    } else {
                        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                        exchange.getResponseSender().send(output.getValue());
                    }
                }
            } catch (Exception exception) {
                exception.getStackTrace();
                String errorStr = OpflowObjectTree.buildMap(false)
                    .put("exceptionClass", exception.getClass().getName())
                    .put("exceptionPayload", OpflowJsonTool.toString(exception))
                    .put("type", exception.getClass().getName())
                    .put("message", exception.getMessage())
                    .toString();
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(errorStr);
            }
        }
    }
    
    class PageNotFoundHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.setStatusCode(404);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Page Not Found");
        }
    }
    
    protected void assertSystemShutdownHook() {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }
}
