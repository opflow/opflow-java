package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import com.devebot.opflow.supports.OpflowEnvtool;
import com.rabbitmq.client.ConnectionFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;

/**
 *
 * @author acegik
 */
public class OpflowExporter {

    public final static String DEFAULT_PROM_EXPORTER_PORT = "9450";
    public final static String DEFAULT_PROM_EXPORTER_PORT_KEY = "opflow.exporter.port";
    public final static String DEFAULT_PROM_EXPORTER_PORT_ENV = "OPFLOW_EXPORTER_PORT";

    public static final String DEFAULT_OPFLOW_PUSH_GATEWAY_JOB = "opflow-push-gateway";
    static final CollectorRegistry pushRegistry = new CollectorRegistry();
    static final PushGateway pushGateway = new PushGateway("localhost:9091");

    private static final Gauge engineConnectionGauge = Gauge.build()
            .name("opflow_engine_connection")
            .help("Number of producing connections.")
            .labelNames("host", "port", "virtual_host", "connection_type")
            .register(pushRegistry);
    
    private static void finish(String jobName) {
        try {
            pushGateway.push(pushRegistry, jobName);
        } catch (IOException exception) {
            // 
        }
    }
    
    public static void incEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        engineConnectionGauge.labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).inc();
        finish(DEFAULT_OPFLOW_PUSH_GATEWAY_JOB);
    }
    
    public static void decEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        engineConnectionGauge.labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).dec();
        finish(DEFAULT_OPFLOW_PUSH_GATEWAY_JOB);
    }

    private static OpflowExporter instance;
    private static final OpflowEnvtool envtool = OpflowEnvtool.instance;

    private static String getExporterPort() {
        String port1 = envtool.getEnvironVariable(DEFAULT_PROM_EXPORTER_PORT_ENV, null);
        String port2 = envtool.getSystemProperty(DEFAULT_PROM_EXPORTER_PORT_KEY, port1);
        return (port2 != null) ? port2 : DEFAULT_PROM_EXPORTER_PORT; 
    }
    
    private OpflowExporter() throws OpflowBootstrapException {
        String portStr = getExporterPort();
        try {
            DefaultExports.initialize();
            HTTPServer server = new HTTPServer(Integer.parseInt(portStr));
        } catch (Exception exception) {
            throw new OpflowBootstrapException("Exporter connection refused, port: " + portStr, exception);
        }
    }
 
    public static OpflowExporter getInstance() throws OpflowBootstrapException {
        if (instance == null) {
            instance = new OpflowExporter();
        }
        return instance;
    }
}
