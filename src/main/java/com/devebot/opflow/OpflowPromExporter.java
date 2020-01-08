package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowEnvtool;
import com.rabbitmq.client.ConnectionFactory;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
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
    
    private final static Logger LOG = LoggerFactory.getLogger(OpflowPromExporter.class);
    
    private Gauge componentInstanceGauge;

    private Gauge assertComponentInstanceGauge() {
        if (componentInstanceGauge == null) {
            Gauge.Builder builder = Gauge.build()
                    .name("opflow_component_instance")
                    .help("Number of component instances.")
                    .labelNames("instance_type", "instance_id");
            componentInstanceGauge = builder.register();
        }
        return componentInstanceGauge;
    }
    
    @Override
    public void updateComponentInstance(String instanceType, String instanceId, GaugeAction action) {
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
    }
    
    @Override
    public void removeComponentInstance(String instanceType, String instanceId) {
        assertComponentInstanceGauge().remove(instanceType, instanceId);
    }
    
    private Gauge engineConnectionGauge;

    private Gauge assertEngineConnectionGauge() {
        if (engineConnectionGauge == null) {
            Gauge.Builder builder = Gauge.build()
                    .name("opflow_engine_connection")
                    .help("Number of active connections.")
                    .labelNames("host", "port", "virtual_host", "connection_type");
            engineConnectionGauge = builder.register();
        }
        return engineConnectionGauge;
    }
    
    @Override
    public void updateEngineConnection(ConnectionFactory factory, String connectionType, GaugeAction action) {
        Gauge.Child metric = assertEngineConnectionGauge().labels(factory.getHost(), String.valueOf(factory.getPort()), factory.getVirtualHost(), connectionType);
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
    }

    private Gauge activeChannelGauge;

    private Gauge assertActiveChannelGauge() {
        if (activeChannelGauge == null) {
            Gauge.Builder builder = Gauge.build()
                    .name("opflow_active_channel")
                    .help("Number of active channels.")
                    .labelNames("instance_type", "instance_id");
            activeChannelGauge = builder.register();
        }
        return activeChannelGauge;
    }
    
    @Override
    public void updateActiveChannel(String instanceType, String instanceId, GaugeAction action) {
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
    }
    
    private Counter rpcInvocationCounter;
    
    private long rpcInvocation_Master = 0;
    private long rpcInvocation_DirectWorker = 0;
    private long rpcInvocation_RemoteWorker = 0;
    
    private Counter assertRpcInvocationCounter() {
        if (rpcInvocationCounter == null) {
            Counter.Builder builder = Counter.build()
                    .name("opflow_rpc_invocation_total")
                    .help("The total of the RPC invocation events")
                    .labelNames(new String[] { "module_name", "event_name", "routine_id", "status" });
            rpcInvocationCounter = builder.register();
        }
        return rpcInvocationCounter;
    }
    
    @Override
    public void countRpcInvocation(String moduleName, String eventName, String routineId, String status) {
        if ("commander".equals(moduleName)) {
            switch (eventName) {
                case "master":
                    rpcInvocation_Master++;
                    break;
                case "direct_worker":
                    rpcInvocation_DirectWorker++;
                    break;
                case "remote_worker":
                    rpcInvocation_RemoteWorker++;
                    break;
                default:
                    break;
            }
        }
        assertRpcInvocationCounter().labels(moduleName, eventName, routineId, status).inc();
    }
    
    @Override
    public double getRpcInvocationTotal(String moduleName, String eventName) {
        if ("commander".equals(moduleName)) {
            switch (eventName) {
                case "master":
                    return rpcInvocation_Master;
                case "direct_worker":
                    return rpcInvocation_DirectWorker;
                case "remote_worker":
                    return rpcInvocation_RemoteWorker;
                default:
                    break;
            }
        }
        return 0;
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
    
    public OpflowPromExporter(Map<String, Object> kwargs) throws OpflowOperationException {
        kwargs = OpflowUtil.ensureNotNull(kwargs);
        
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
}
