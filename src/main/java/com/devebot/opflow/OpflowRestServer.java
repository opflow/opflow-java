package com.devebot.opflow;

import com.devebot.opflow.OpflowLogTracer.Level;
import com.devebot.opflow.supports.OpflowJsonTool;
import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowConverter;
import com.devebot.opflow.supports.OpflowObjectTree;
import com.devebot.opflow.supports.OpflowSystemInfo;
import io.undertow.Undertow;
import io.undertow.security.api.AuthenticationMechanism;
import io.undertow.security.api.AuthenticationMode;
import io.undertow.security.handlers.AuthenticationCallHandler;
import io.undertow.security.handlers.AuthenticationConstraintHandler;
import io.undertow.security.handlers.AuthenticationMechanismsHandler;
import io.undertow.security.handlers.SecurityInitialHandler;
import io.undertow.security.impl.BasicAuthenticationMechanism;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.server.RoutingHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.handlers.GracefulShutdownHandler;
import io.undertow.server.handlers.RedirectHandler;
import io.undertow.server.handlers.cache.DirectBufferCache;
import io.undertow.server.handlers.resource.CachingResourceManager;
import io.undertow.server.handlers.resource.ClassPathResourceManager;
import io.undertow.server.handlers.resource.ResourceHandler;
import io.undertow.server.handlers.resource.ResourceManager;
import io.undertow.util.Headers;
import io.undertow.util.PathTemplateMatch;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowRestServer implements AutoCloseable {
    private final static Logger LOG = LoggerFactory.getLogger(OpflowRestServer.class);

    private final String componentId;
    private final OpflowLogTracer logTracer;
    private final Map<String, OpflowConnector> connectors;
    private final OpflowInfoCollector infoCollector;
    private final OpflowTaskSubmitter taskSubmitter;
    private final RoutingHandler defaultHandlers;
    private final String host;
    private final Integer port;
    private final String[] credentials;
    private final Boolean enabled;
    private final long shutdownTimeout;
    private final boolean autoShutdown;
    private final Thread shutdownHook;
    private Undertow server;
    private GracefulShutdownHandler shutdownHandler;
    private OpflowIdentityManager identityManager;
    
    private final Object threadExecutorLock = new Object();
    private ExecutorService threadExecutor = null;
    
    OpflowRestServer(Map<String, OpflowConnector> _connectors,
            OpflowInfoCollector _infoCollector,
            OpflowTaskSubmitter _taskSubmitter,
            Map<String, Object> kwargs) throws OpflowBootstrapException {
        kwargs = OpflowObjectTree.ensureNonNull(kwargs);
        
        componentId = OpflowUtil.getStringField(kwargs, OpflowConstant.COMPONENT_ID, true);
        enabled = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_ENABLED, null);
        host = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_HOST, "0.0.0.0");
        port = OpflowUtil.detectFreePort(kwargs, OpflowConstant.OPFLOW_COMMON_PORTS, new Integer[] {
                8989, 8990, 8991, 8992, 8993, 8994, 8995, 8996, 8997, 8998, 8999
        });
        credentials = OpflowUtil.getStringArray(kwargs, OpflowConstant.OPFLOW_COMMON_CREDENTIALS, null);
        
        shutdownTimeout = OpflowUtil.getLongField(kwargs, OpflowConstant.OPFLOW_COMMON_SHUTDOWN_TIMEOUT, 1000l);
        
        autoShutdown = OpflowUtil.getBooleanField(kwargs, OpflowConstant.OPFLOW_COMMON_AUTO_SHUTDOWN, Boolean.FALSE);
        
        if (autoShutdown) {
            shutdownHook = new Thread() {
                @Override
                public void run() {
                    close();
                }
            };
        } else {
            shutdownHook = null;
        }
        
        identityManager = new OpflowIdentityManager(credentials);
        
        logTracer = OpflowLogTracer.ROOT.branch("restServerId", componentId);
        
        connectors = _connectors;
        infoCollector = _infoCollector;
        taskSubmitter = _taskSubmitter;
        
        ExecHandler execHandler = new ExecHandler();
        
        InfoHandler infoHandler = new InfoHandler();
        TrafficHandler trafficHandler = new TrafficHandler();
        PingHandler pingHandler = new PingHandler();
        
        defaultHandlers = new RoutingHandler()
                .get("/info", infoHandler)
                .get("/exec/{action}", new BlockingHandler(execHandler))
                .get("/traffic", trafficHandler)
                .put("/traffic", new BlockingHandler(trafficHandler))
                .get("/ping", pingHandler);
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
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() is disable")
                    .stringify());
            return;
        }
        if (port == null) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() all of the ports are busy")
                    .stringify());
            return;
        }
        if (extraHandlers != null || kwargs != null) {
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .text("RestServer[${restServerId}].serve() stop the current service")
                    .stringify());
            this.close();
        }

        assertSystemShutdownHook();

        synchronized (this) {
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

                shutdownHandler = new GracefulShutdownHandler(new RequestLoggingHandler(routes));
                
                server = Undertow.builder()
                        .addHttpListener(port, host)
                        .setHandler(addSecurity(shutdownHandler, identityManager))
                        .build();

                if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                        .text("RestServer[${restServerId}].serve() a new HTTP server is created")
                        .stringify());
            }
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                .put("port", port)
                .put("host", host)
                .text("RestServer[${restServerId}].serve() Server listening on (http://${host}:${port})")
                .stringify());
            server.start();
        }
    }
    
    @Override
    public synchronized void close() {
        releaseThreadExecutor();
        if (server != null) {
            if (shutdownHandler != null) {
                shutdownHandler.shutdown();
                try {
                    if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                            .text("RestServer[${restServerId}].close() shutdownHandler.awaitShutdown() starting")
                            .stringify());
                    if (shutdownHandler.awaitShutdown(shutdownTimeout)) {
                        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                                .text("RestServer[${restServerId}].close() shutdownHandler.awaitShutdown() has done")
                                .stringify());
                    } else {
                        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                                .text("RestServer[${restServerId}].close() shutdownHandler.awaitShutdown() is timeout")
                                .stringify());
                    }
                }
                catch (InterruptedException ex) {
                    if (logTracer.ready(LOG, Level.ERROR)) LOG.error(logTracer
                            .text("RestServer[${restServerId}].close() shutdownHandler.awaitShutdown() interrupted")
                            .stringify());
                    shutdownHandler.shutdown();
                }
                finally {
                    if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
                            .text("RestServer[${restServerId}].close() shutdownHandler.awaitShutdown() finished")
                            .stringify());
                }
                shutdownHandler = null;
            }
            server.stop();
            server = null;
        }
    }
    
    private ExecutorService getThreadExecutor() {
        if (threadExecutor == null) {
            synchronized (threadExecutorLock) {
                if (threadExecutor == null) {
                    threadExecutor = Executors.newCachedThreadPool();
                }
            }
        }
        return threadExecutor;
    }
    
    private void releaseThreadExecutor() {
        synchronized (threadExecutorLock) {
            if (threadExecutor != null) {
                threadExecutor.shutdown();
                try {
                    if (!threadExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                        threadExecutor.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    threadExecutor.shutdownNow();
                }
                finally {
                    threadExecutor = null;
                }
            }
        }
    }
    
    private static HttpHandler addSecurity(final HttpHandler toWrap, final OpflowIdentityManager identityManager) {
        if (identityManager == null || !identityManager.isActive()) {
            return toWrap;
        }
        HttpHandler handler = toWrap;
        handler = new AuthenticationCallHandler(handler);
        handler = new AuthenticationConstraintHandler(handler);
        final List<AuthenticationMechanism> mechanisms = Collections.<AuthenticationMechanism>singletonList(new BasicAuthenticationMechanism("Opflow-Realm"));
        handler = new AuthenticationMechanismsHandler(handler, mechanisms);
        handler = new SecurityInitialHandler(AuthenticationMode.PRO_ACTIVE, identityManager, handler);
        return handler;
    }
    
    protected void assertSystemShutdownHook() {
        if (shutdownHook != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }
    
    public HttpHandler buildResourceHandler(String prefix) {
        return buildResourceHandler(prefix, 30000);
    }
    
    public HttpHandler buildResourceHandler(String prefix, int cacheTime) {
        if (logTracer.ready(LOG, Level.DEBUG)) LOG.debug(logTracer
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
    
    class RequestLoggingHandler implements HttpHandler {
        private final HttpHandler next;

        public RequestLoggingHandler(final HttpHandler next) {
            this.next = next;
        }
        
        @Override
        public void handleRequest(final HttpServerExchange exchange) throws Exception {
            Set<String> roles = OpflowIdentityManager.getRoles(exchange);
            String relativePath = exchange.getRelativePath();
            
            if (logTracer.ready(LOG, Level.INFO)) LOG.info(logTracer
                    .put("username", OpflowIdentityManager.getUsername(exchange))
                    .put("roles", roles)
                    .put("method", exchange.getRequestMethod())
                    .put("path", exchange.getRelativePath())
                    .text("RestServer[${restServerId}] - User[${username}] with roles[${roles}] invokes [${method}] ${path}")
                    .stringify());
            
            if (checkPermission(relativePath, roles)) {
                next.handleRequest(exchange);
            } else {
                exchange.setStatusCode(403);
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.getResponseSender().send("Insufficient permissions");
            }
        }
    }
    
    class ExecHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                PathTemplateMatch pathMatch = exchange.getAttachment(PathTemplateMatch.ATTACHMENT_KEY);
                Map<String, Object> result = OpflowObjectTree.buildMap().toMap();
                String action = pathMatch.getParameters().get("action");
                if (action != null && action.length() > 0) {
                    switch(action) {
                        case "gc":
                            Runtime.getRuntime().gc();
                            result = OpflowSystemInfo.getMemUsage().toMap(getPrettyParam(exchange));
                            break;

                        case "pause":
                            Long period = getQueryParam(exchange, "period", Long.class, null);
                            if (period == null) {
                                period = getQueryParam(exchange, "duration", Long.class, 10000l);
                            }
                            result = taskSubmitter.pause(period);
                            result.put("message", "Pausing in " + period + " milliseconds");
                            break;

                        case "unpause":
                            result = taskSubmitter.unpause();
                            break;

                        case "reset":
                            result = taskSubmitter.reset();
                            break;

                        case "activate-publisher":
                            boolean state_ = getQueryParam(exchange, "state", Boolean.class, true);
                            result = taskSubmitter.activateAllPublishers(state_, OpflowObjectTree.buildMap(false)
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;

                        case "activate-remote-worker":
                        case "activate-detached-worker":
                        case "activate-remote-amqp-worker":
                            boolean state0 = getQueryParam(exchange, "state", Boolean.class, true);
                            result = taskSubmitter.activateAllRemoteAMQPWorkers(state0, OpflowObjectTree.buildMap(false)
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;

                        case "activate-remote-http-worker":
                            boolean state1 = getQueryParam(exchange, "state", Boolean.class, true);
                            result = taskSubmitter.activateAllRemoteHTTPWorkers(state1, OpflowObjectTree.buildMap(false)
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;
                            
                        case "activate-backup-worker":
                        case "activate-direct-worker":
                        case "activate-embedded-worker":
                        case "activate-reserved-worker":
                        case "activate-native-worker":
                            boolean state2 = getQueryParam(exchange, "state", Boolean.class, true);
                            result = taskSubmitter.activateAllNativeWorkers(state2, OpflowObjectTree.buildMap(false)
                                    .put("class", getQueryParam(exchange, "class"))
                                    .toMap());
                            break;
                            
                        case "reset-counter":
                            result = taskSubmitter.resetRpcInvocationCounter();
                            break;
                            
                        case "reset-discovery-client":
                            result = taskSubmitter.resetDiscoveryClient();
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
        private Map<String, Object> info() {
            Map<String, Boolean> opts = new HashMap<>();
            opts.put(OpflowInfoCollector.SCOPE_INFO, true);
            return infoCollector.collect(opts);
        }
    }
    
    class TrafficHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                Map<String, Object> body = null;
                if (exchange.getRequestMethod().equalToString("PUT")) {
                    body = OpflowJsonTool.toObjectMap(exchange.getInputStream());
                }
                Map<String, Object> result = traffic(body);
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(OpflowJsonTool.toString(result, getPrettyParam(exchange)));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
        private Map<String, Object> traffic(Map<String, Object> kwargs) {
            Map<String, Boolean> opts = new HashMap<>();
            opts.put(OpflowInfoCollector.SCOPE_INFO, true);
            if (kwargs != null) {
                for (Map.Entry<String, Object> entry : kwargs.entrySet()) {
                    if (entry.getValue() instanceof Boolean) {
                        opts.put(entry.getKey(), (Boolean) entry.getValue());
                    }
                }
            }
            return infoCollector.traffic(opts);
        }
    }
    
    private final static Map<String, Boolean> PING_FLAG = OpflowObjectTree.<Boolean>buildMap()
            .put(OpflowInfoCollector.SCOPE_PING, true)
            .toMap();
    
    class PingHandler implements HttpHandler {
        @Override
        public void handleRequest(HttpServerExchange exchange) throws Exception {
            try {
                boolean ok = true;
                Map<String, OpflowRpcChecker.Info> result = ping();
                Map<String, Object> output = OpflowObjectTree.buildMap().toMap();
                for (Map.Entry<String, OpflowRpcChecker.Info> entry : result.entrySet()) {
                    OpflowRpcChecker.Info info = entry.getValue();
                    if (info != null) {
                        if (!"ok".equals(info.getStatus())) {
                            ok = false;
                        }
                        output.put(entry.getKey(), info.toMap());
                    }
                }
                if (!ok) {
                    exchange.setStatusCode(503);
                }
                // render the result
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/json");
                exchange.getResponseSender().send(OpflowJsonTool.toString(output, getPrettyParam(exchange)));
            } catch (Exception exception) {
                exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
                exchange.setStatusCode(500).getResponseSender().send(exception.toString());
            }
        }
        
        private Map<String, OpflowRpcChecker.Info> ping() {
            Map<String, OpflowRpcChecker.Info> result = OpflowObjectTree.<OpflowRpcChecker.Info>buildMap().toMap();
            // build the tasks
            List<Callable<OpflowRpcChecker.Cover>> tasks = new ArrayList<>();
            for(Map.Entry<String, OpflowConnector> entry : connectors.entrySet()) {
                final String name = entry.getKey();
                final Map<String, Object> me = infoCollector.collect(name, PING_FLAG);
                final OpflowConnector connector = entry.getValue();
                if (connector == null) continue;
                final OpflowRpcChecker rpcChecker = connector.getRpcChecker();
                if (rpcChecker == null) continue;
                tasks.add(new Callable() {
                    @Override
                    public OpflowRpcChecker.Cover call() throws Exception {
                        try {
                            return new OpflowRpcChecker.Cover(name, new OpflowRpcChecker.Info(me, rpcChecker.send(null)));
                        }
                        catch (Throwable exception) {
                            return new OpflowRpcChecker.Cover(name, new OpflowRpcChecker.Info(me, exception));
                        }
                    }
                });
            }
            // invoke the tasks
            try {
                List<Future<OpflowRpcChecker.Cover>> futures = getThreadExecutor().invokeAll(tasks);
                for (Future<OpflowRpcChecker.Cover> future: futures) {
                    try {
                        OpflowRpcChecker.Cover cover = future.get();
                        result.put(cover.getName(), cover.getBody());
                    }
                    catch (Exception ee) {
                        // Skip the exception
                    }
                }
            }
            catch (InterruptedException ie) {
                throw new OpflowOperationException(ie);
            }
            // return the result
            return result;
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
    
    private static boolean checkPermission(String path, Set<String> roles) {
        if (path == null) {
            return true;
        }
        switch(path) {
            case "/traffic":
                if (roles.isEmpty()) {
                    return true;
                }
                return roles.contains("administrator") || roles.contains("monitoring");
            case "/info":
            case "/ping":
                return roles.contains("administrator") || roles.contains("monitoring");
        }
        if (path.startsWith("/exec/")) {
            return roles.contains("administrator");
        }
        return true;
    }
}
