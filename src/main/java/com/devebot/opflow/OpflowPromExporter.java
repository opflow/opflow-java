package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowOperationException;
import com.devebot.opflow.supports.OpflowObjectTree;
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
    
    private final String host;
    private final Integer port;
    
    private Gauge componentInstanceGauge;
    private Gauge engineConnectionGauge;
    private Counter rpcInvocationCounter;
    
    public OpflowPromExporter(Map<String, Object> kwargs) throws OpflowOperationException {
        kwargs = OpflowObjectTree.ensureNonNull(kwargs);
        
        // get the host from configuration
        host = OpflowUtil.getStringField(kwargs, OpflowConstant.OPFLOW_COMMON_HOST, "0.0.0.0");
        
        // detect the avaiable port
        port = OpflowUtil.detectFreePort(kwargs, OpflowConstant.OPFLOW_COMMON_PORTS, new Integer[] {
                9450, 9451, 9452, 9453, 9454, 9455, 9456, 9457, 9458, 9459
        });
        
        if (port != null) {
            try {
                DefaultExports.initialize();
                HTTPServer server = new HTTPServer(host, port);
            } catch (IOException exception) {
                throw new OpflowOperationException("Exporter connection refused, port: " + port, exception);
            }
        }
    }
    
    private Gauge assertComponentInstanceGauge() {
        if (componentInstanceGauge == null) {
            Gauge.Builder builder = Gauge.build()
                    .name("opflow_component_instance")
                    .help("Number of component instances.")
                    .labelNames("instance_id", "component_type");
            componentInstanceGauge = builder.register();
        }
        return componentInstanceGauge;
    }
    
    @Override
    public void updateComponentInstance(String componentType, String componentId, GaugeAction action) {
        Gauge.Child metric = assertComponentInstanceGauge().labels(OpflowLogTracer.getInstanceId(), componentType);
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
    
    private Gauge assertEngineConnectionGauge() {
        if (engineConnectionGauge == null) {
            Gauge.Builder builder = Gauge.build()
                    .name("opflow_engine_connection")
                    .help("Number of active connections.")
                    .labelNames("instance_id", "connection_owner", "connection_type");
            engineConnectionGauge = builder.register();
        }
        return engineConnectionGauge;
    }
    
    @Override
    public void updateEngineConnection(ConnectionFactory factory, String connectionOwner, String connectionType, GaugeAction action) {
        Gauge.Child metric = assertEngineConnectionGauge().labels(OpflowLogTracer.getInstanceId(), connectionOwner, connectionType);
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

    private Counter assertRpcInvocationCounter() {
        if (rpcInvocationCounter == null) {
            Counter.Builder builder = Counter.build()
                    .name("opflow_rpc_invocation_total")
                    .help("The total of the RPC invocation events")
                    .labelNames("instance_id", "component_type", "flow_name", "routine_id", "status");
            rpcInvocationCounter = builder.register();
        }
        return rpcInvocationCounter;
    }
    
    @Override
    public void countRpcInvocation(String componentType, String flowName, String routineSignature, String status) {
        assertRpcInvocationCounter().labels(OpflowLogTracer.getInstanceId(), componentType, flowName, routineSignature, status).inc();
    }
    
    @Override
    public RpcInvocationCounter getRpcInvocationCounter(String moduleName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Object> resetRpcInvocationCounter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
