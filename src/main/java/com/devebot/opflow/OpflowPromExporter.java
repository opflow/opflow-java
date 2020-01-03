package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowEnvtool;
import com.rabbitmq.client.ConnectionFactory;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author topops
 */
public class OpflowPromExporter extends OpflowPromMeasurer {
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPromMeasurer.class);
    
    private final CollectorRegistry pushRegistry = new CollectorRegistry();
    private PushGateway pushGateway;

    private void finish(String jobName) {
        if (pushGateway != null) {
            try {
                pushGateway.push(pushRegistry, DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
            } catch (IOException exception) {}
        }
    }
    
    private Gauge componentInstanceGauge;

    private Gauge assertComponentInstanceGauge() {
        if (componentInstanceGauge == null) {
            Gauge.Builder builder = Gauge.build()
            .name("opflow_component_instance")
            .help("Number of component instances.")
            .labelNames("instance_type", "instance_id");
            if (pushGateway != null) {
                componentInstanceGauge = builder.register(pushRegistry);
            } else {
                componentInstanceGauge = builder.register();
            }
        }
        return componentInstanceGauge;
    }
    
    @Override
    public void changeComponentInstance(String instanceType, String instanceId, GaugeAction action) {
        Gauge.Child metric = assertComponentInstanceGauge().labels(instanceType, instanceId);
        switch(action) {
            case INC:
                metric.inc();
                break;
            case DEC:
                metric.dec();
                break;
            default:
                break;
        }
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    @Override
    public void removeComponentInstance(String instanceType, String instanceId) {
        if (pushGateway != null) {
            assertComponentInstanceGauge().remove(instanceType, instanceId);
        }
    }
    
    private Gauge engineConnectionGauge;

    private Gauge assertEngineConnectionGauge() {
        if (engineConnectionGauge == null) {
            Gauge.Builder builder = Gauge.build()
            .name("opflow_engine_connection")
            .help("Number of active connections.")
            .labelNames("host", "port", "virtual_host", "connection_type");
            if (pushGateway != null) {
                engineConnectionGauge = builder.register(pushRegistry);
            } else {
                engineConnectionGauge = builder.register();
            }
        }
        return engineConnectionGauge;
    }
    
    @Override
    public void incEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        assertEngineConnectionGauge().labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).inc();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    @Override
    public void decEngineConnectionGauge(ConnectionFactory factory, String connectionType) {
        assertEngineConnectionGauge().labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType).dec();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }

    private Gauge activeChannelGauge;

    private Gauge assertActiveChannelGauge() {
        if (activeChannelGauge == null) {
            Gauge.Builder builder = Gauge.build()
            .name("opflow_active_channel")
            .help("Number of active channels.")
            .labelNames("instance_type", "instance_id");
            if (pushGateway != null) {
                activeChannelGauge = builder.register(pushRegistry);
            } else {
                activeChannelGauge = builder.register();
            }
        }
        return activeChannelGauge;
    }
    
    @Override
    public void changeActiveChannel(String instanceType, String instanceId, GaugeAction action) {
        Gauge.Child metric = assertActiveChannelGauge().labels(instanceType, instanceId);
        switch(action) {
            case INC:
                metric.inc();
                break;
            case DEC:
                metric.dec();
                break;
            default:
                break;
        }
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    private Counter rpcInvocationCounterGauge;
    
    private final String[] rpcInvocationEventLabels = new String[] { "module_name", "engineId", "routineId", "status" };
    
    private Counter assertRpcInvocationCounterGauge() {
        if (rpcInvocationCounterGauge == null) {
            Counter.Builder builder = Counter.build()
                .name("opflow_rpc_invocation_total")
                .help("The total of the RPC invocation events")
                .labelNames(rpcInvocationEventLabels);
            if (pushGateway != null) {
                rpcInvocationCounterGauge = builder.register(pushRegistry);
            } else {
                rpcInvocationCounterGauge = builder.register();
            }
        }
        return rpcInvocationCounterGauge;
    }
    
    private Counter.Child getRpcInvocationChildEvent(Map<String, String> labels) {
        String[] values = new String[rpcInvocationEventLabels.length];
        for (int i=0; i<rpcInvocationEventLabels.length; i++) {
            values[i] = labels.get(rpcInvocationEventLabels[i]);
        }
        return assertRpcInvocationCounterGauge().labels(values);
    }
    
    @Override
    public void incRpcInvocationEvent(String module_name, String engineId, String routineId, String status) {
        assertRpcInvocationCounterGauge().labels(module_name, engineId, routineId, status).inc();
        finish(DEFAULT_PROM_PUSHGATEWAY_JOBNAME);
    }
    
    private static String getExporterPort() {
        String port1 = OpflowEnvtool.instance.getEnvironVariable(DEFAULT_PROM_EXPORTER_PORT_ENV, null);
        String port2 = OpflowEnvtool.instance.getSystemProperty(DEFAULT_PROM_EXPORTER_PORT_KEY, port1);
        return ("default".equals(port2) ? DEFAULT_PROM_EXPORTER_PORT_VAL : port2);
    }
    
    private static String getPushGatewayAddr() {
        String addr1 = OpflowEnvtool.instance.getEnvironVariable(DEFAULT_PROM_PUSHGATEWAY_ADDR_ENV, null);
        String addr2 = OpflowEnvtool.instance.getSystemProperty(DEFAULT_PROM_PUSHGATEWAY_ADDR_KEY, addr1);
        return ("default".equals(addr2) ? DEFAULT_PROM_PUSHGATEWAY_ADDR_VAL : addr2);
    }
    
    public OpflowPromExporter() throws OpflowOperationException {
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
            if (portStr != null) {
                try {
                    DefaultExports.initialize();
                    HTTPServer server = new HTTPServer(Integer.parseInt(portStr));
                } catch (IOException | NumberFormatException exception) {
                    throw new OpflowOperationException("Exporter connection refused, port: " + portStr, exception);
                }
            }
        }
        
        // Initialize the metrics
        assertEngineConnectionGauge();
    }
}
