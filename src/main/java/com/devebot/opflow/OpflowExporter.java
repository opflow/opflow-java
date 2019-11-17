package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowEnvtool;
import com.rabbitmq.client.ConnectionFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author acegik
 */
public class OpflowExporter {

    private final static Logger LOG = LoggerFactory.getLogger(OpflowExporter.class);
    
    public final static String DEFAULT_PROM_EXPORTER_PORT_VAL = "9450";
    public final static String DEFAULT_PROM_EXPORTER_PORT_KEY = "opflow.exporter.port";
    public final static String DEFAULT_PROM_EXPORTER_PORT_ENV = "OPFLOW_EXPORTER_PORT";

    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_VAL = "localhost:9091";
    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_KEY = "opflow.pushgateway.addr";
    public final static String DEFAULT_PROM_PUSHGATEWAY_ADDR_ENV = "OPFLOW_PUSHGATEWAY_ADDR";
    public static final String DEFAULT_PROM_PUSHGATEWAY_JOBNAME = "opflow-push-gateway";

    private static OpflowExporter instance;

    private final CollectorRegistry pushRegistry = new CollectorRegistry();
    private PushGateway pushGateway;

    private void finish(String jobName) {
        if (pushGateway != null) {
            try {
                pushGateway.push(pushRegistry, DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
            } catch (IOException exception) {}
        }
    }
    
    private Gauge engineConnectionGauge;

    private Gauge assertEngineConnectionGauge() {
        if (engineConnectionGauge == null) {
            Gauge.Builder builder = Gauge.build()
            .name("opflow_engine_connection")
            .help("Number of producing connections.")
            .labelNames("host", "port", "virtual_host", "connection_type");
            if (pushGateway != null) {
                engineConnectionGauge = builder.register(pushRegistry);
            } else {
                engineConnectionGauge = builder.register();
            }
        }
        return engineConnectionGauge;
    }
    
    public void incEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        assertEngineConnectionGauge().labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).inc();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    public void decEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        assertEngineConnectionGauge().labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).dec();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }

    private Gauge rpcInvocationEventGauge;
    
    private Gauge assertRpcInvocationEventGauge() {
        if (rpcInvocationEventGauge == null) {
            Gauge.Builder builder = Gauge.build()
                .name("opflow_rpc_invocation_events")
                .help("The timestamp of the RPC invocation events")
                .labelNames("module_name", "requestId", "routineId", "taskId", "status");
            if (pushGateway != null) {
                rpcInvocationEventGauge = builder.register(pushRegistry);
            } else {
                rpcInvocationEventGauge = builder.register();
            }
        }
        return rpcInvocationEventGauge;
    }
    
    public void setRpcInvocationEventGauge(String moduleName, String requestId, String routineId, String taskId, String status) {
        assertRpcInvocationEventGauge().labels(moduleName, requestId, routineId, taskId, status).setToCurrentTime();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    public void dropRpcInvocationEventGauge(String moduleName, String requestId, String routineId, String taskId, String status) {
        assertRpcInvocationEventGauge().remove(moduleName, requestId, routineId, taskId, status);
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    private static String getExporterPort() {
        String port1 = OpflowEnvtool.instance.getEnvironVariable(DEFAULT_PROM_EXPORTER_PORT_ENV, null);
        String port2 = OpflowEnvtool.instance.getSystemProperty(DEFAULT_PROM_EXPORTER_PORT_KEY, port1);
        return (port2 != null) ? port2 : DEFAULT_PROM_EXPORTER_PORT_VAL;
    }
    
    private static String getPushGatewayAddr() {
        String addr1 = OpflowEnvtool.instance.getEnvironVariable(DEFAULT_PROM_PUSHGATEWAY_ADDR_ENV, null);
        String addr2 = OpflowEnvtool.instance.getSystemProperty(DEFAULT_PROM_PUSHGATEWAY_ADDR_KEY, addr1);
        return ("default".equals(addr2) ? DEFAULT_PROM_PUSHGATEWAY_ADDR_VAL : addr2);
    }
    
    private OpflowExporter() throws OpflowOperationException {
        // Initialize the PushGateway
        String pushAddr = getPushGatewayAddr();
        if (pushAddr != null) {
            pushGateway = new PushGateway(pushAddr);
            if (OpflowLogTracer.has(LOG, "info")) LOG.info("Exporter - pushgateway address: " + pushAddr);
        } else {
            if (OpflowLogTracer.has(LOG, "info")) LOG.info("Exporter - pushgateway is empty");
        }

        // Initialize the HTTP Exporter
        if (pushGateway == null) {
            String portStr = getExporterPort();
            try {
                DefaultExports.initialize();
                HTTPServer server = new HTTPServer(Integer.parseInt(portStr));
            } catch (IOException | NumberFormatException exception) {
                throw new OpflowOperationException("Exporter connection refused, port: " + portStr, exception);
            }
        }
        
        // Initialize the metrics
        assertEngineConnectionGauge();
    }
 
    public static OpflowExporter getInstance() throws OpflowOperationException {
        if (instance == null) {
            instance = new OpflowExporter();
        }
        return instance;
    }
}
