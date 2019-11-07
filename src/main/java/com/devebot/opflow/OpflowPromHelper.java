package com.devebot.opflow;

import com.rabbitmq.client.ConnectionFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import java.io.IOException;

/**
 *
 * @author acegik
 */
public class OpflowPromHelper {
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
}
