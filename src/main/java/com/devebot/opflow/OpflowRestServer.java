package com.devebot.opflow;

import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowConverter;
import com.devebot.opflow.supports.OpflowNetTool;
import io.undertow.Undertow;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.handlers.RedirectHandler;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import java.nio.file.Paths;
import java.util.Deque;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowRestServer implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRestServer.class);

    private final String instanceId;
    private final OpflowLogTracer logTracer;
    private final OpflowInfoCollector infoCollector;
    private final OpflowTaskSubmitter taskSubmitter;
    private final OpflowRpcChecker rpcChecker;
    private final RoutingHandler defaultHandlers;
    private final String host;
    private final Integer port;
    private final Boolean enabled;
    private Undertow server;

    OpflowRestServer(OpflowInfoCollector _infoCollector,
            OpflowTaskSubmitter _taskSubmitter,
            OpflowRpcChecker _rpcChecker,
            Map<String, Object> kwargs) throws OpflowBootstrapException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);

        instanceId = OpflowUtil.getOptionField(kwargs, "instanceId", true);
        enabled = OpflowConverter.convert(OpflowUtil.getOptionField(kwargs, "enabled", null), Boolean.class);
        host = OpflowUtil.getOptionField(kwargs, "host", "0.0.0.0").toString();
        
        // detect the avaiable port
        Integer[] ports = OpflowConverter.convert(OpflowUtil.getOptionField(kwargs, "ports", new Integer[] {
            8989, 8990, 8991, 8992, 8993, 8994, 8995, 8996, 8997, 8998, 8999
        }), (new Integer[0]).getClass());
        port = OpflowNetTool.detectFreePort(ports);
        
        logTracer = OpflowLogTracer.ROOT.branch("restServerId", instanceId);
        
        infoCollector = _infoCollector;
        taskSubmitter = _taskSubmitter;
        rpcChecker = _rpcChecker;
        
        defaultHandlers = new RoutingHandler();
        defaultHandlers.get("/exec/{action}", new BlockingHandler(new ExecHandler()))
                .get("/info", new InfoHandler())
                .get("/ping", new PingHandler());
    }

    public Map<String, Object> info() {
        return infoCollector.collect(OpflowInfoCollector.Scope.FULL);
    }
    
    public OpflowRpcChecker.Info ping() {
        Map<String, Object> me = infoCollector.collect(OpflowInfoCollector.Scope.BASIC);
        try {
            return new OpflowRpcChecker.Info(me, this.rpcChecker.send(new OpflowRpcChecker.Ping()));
        } catch (Throwable exception) {
            return new OpflowRpcChecker.Info(me, exception);
        }
    }
    
    public RoutingHandler getDefaultHandlers() {
        return defaultHandlers;
    }
    
    public void serve() {
        serve(null, null);
    }

    public void serve(RoutingHandler httpHandlers) {
        serve(httpHandlers, null);
    }
    
    public void serve(RoutingHandler extraHandlers, Map<String, Object> kwargs) {
        if (enabled != null && Boolean.FALSE.equals(enabled)) {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() is disable")
                    .stringify());
            return;
        }
        if (port == null) {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() all of the ports are busy")
                    .stringify());
            return;
        }
        if (extraHandlers != null || kwargs != null) {
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() stop the current service")
                    .stringify());
            this.close();
        }
        if (server == null) {
            RoutingHandler routes = new RoutingHandler();
            
            if (extraHandlers != null) {
                routes.addAll(extraHandlers);
            }
            
            routes.addAll(defaultHandlers)
                    .get("/opflow.yaml", buildResourceHandler("/openapi-spec"))
                    .get("/api-ui/*", buildResourceHandler("/openapi-ui"))
                    .get("/api-ui/", new RedirectHandler("/api-ui/index.html"))
                    .get("/", new RedirectHandler("/api-ui/"))
                    .setFallbackHandler(new PageNotFoundHandler());
                    
            server = Undertow.builder()
                    .addHttpListener(port, host)
                    .setHandler(routes)
                    .build();
            
            if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() a new HTTP server is created")
                    .stringify());
        }
        if (logTracer.ready(LOG, "info")) LOG.info(logTracer
                .put("port", port)
                .put("host", host)
                .text("RestServer[${restServerId}].serve() Server listening on (http://${host}:${port})")
                .stringify());
        server.start();
    }
    
    @Override
    public void close() {
        if (server != null) {
            server.stop();
            server = null;
        }
    }
    
    public HttpHandler buildResourceHandler(String prefix) {
        return buildResourceHandler(prefix, 30000);
    }
    
    public HttpHandler buildResourceHandler(String prefix, int cacheTime) {
        if (logTracer.ready(LOG, "debug")) LOG.debug(logTracer
                .put("prefix", prefix)
                .text("Using ClasspathPathResourceManager with prefix: ${prefix}")
                .stringify());
        ResourceManager classPathManager = new ClassPathResourceManager(OpflowRestServer.class.getClassLoader(),
                Paths.get("META-INF/resources", prefix).toString());
        ResourceManager resourceManager = new CachingResourceManager(100, 65536,
                new DirectBufferCache(1024, 10, 10480),
                classPathManager,
                cacheTime);
        ResourceHandler handler = new ResourceHandler(resourceManager);
        handler.setCacheTime(cacheTime);
        return handler;
    }
    
    class PageNotFoundHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            exchange.setStatusCode(404);
            exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
            exchange.getResponseSender().send("Page Not Found");
        }
    }
    
    class ExecHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                PathTemplateMatch pathMatch = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                Map<String, Object> result = OpflowUtil.buildOrderedMap().toMap();
                String action = pathMatch.getParameters().get("action");
                if (action != null && action.length() > 0) {
                    switch(action) {
                        case "pause":
                            Long duration = getQueryParam(exchange, "duration", Long.class, 10000l);
                            result = taskSubmitter.pause(duration);
                            result.put("message", "Pausing in " + duration + " milliseconds");
                            break;
                        case "unpause":
                            result = taskSubmitter.unpause();
                            break;
                        case "reset":
                            result = taskSubmitter.reset();
                            break;
                        case "activate-remote-worker":
                        case "activate-detached-worker":
                            boolean state1 = getQueryParam(exchange, "value", Boolean.class, true);
                            result = taskSubmitter.activateDetachedWorker(state1, OpflowUtil.buildMap()
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;
                        case "activate-backup-worker":
                        case "activate-direct-worker":
                        case "activate-reserved-worker":
                            boolean state2 = getQueryParam(exchange, "value", Boolean.class, true);
                            result = taskSubmitter.activateReservedWorker(state2, OpflowUtil.buildMap()
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;
                        default:
                            break;
                    }
                }
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(OpflowJsonTool.toString(result, getPrettyParam(exchange)));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
    }
    
    class InfoHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                Map<String, Object> result = info();
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(OpflowJsonTool.toString(result, getPrettyParam(exchange)));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
    }
    
    class PingHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                OpflowRpcChecker.Info result = ping();
                if (!"ok".equals(result.getStatus())) {
                    exchange.setStatusCode(503);
                }
                // render the result
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(result.toString(getPrettyParam(exchange)));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
    }
    
    private boolean getPrettyParam(HttpServerExchange exchange) {
        return getQueryParam(exchange, "pretty", Boolean.class, Boolean.FALSE);
    }
    
    private String getQueryParam(HttpServerExchange exchange, String name) {
        return getQueryParam(exchange, name, String.class, null);
    }
    
    private <T> T getQueryParam(HttpServerExchange exchange, String name, Class<T> type, T defaultVal) {
        Deque<String> vals = exchange.getQueryParameters().get(name);
        if (vals != null && !vals.isEmpty()) {
            return OpflowConverter.convert(vals.getFirst(), type);
        }
        return defaultVal;
    }
}
