package com.devebot.opflow;

import com.devebot.opflow.exception.OpflowBootstrapException;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 *
 * @author acegik
 */
public class OpflowExporter {
    
    private static OpflowExporter instance;

    private OpflowExporter() throws OpflowBootstrapException {
        try {
            DefaultExports.initialize();
            HTTPServer server = new HTTPServer(9450);
        } catch (Exception exception) {
            throw new OpflowBootstrapException("Exporter connection refused, invalid connection parameters", exception);
        }
    }
 
    public static OpflowExporter getInstance() throws OpflowBootstrapException {
        if (instance == null) {
            instance = new OpflowExporter();
        }
        return instance;
    }
}
