package com.devebot.opflow.supports;

import java.io.IOException;
import java.net.ServerSocket;

/**
 *
 * @author drupalex
 */
public class OpflowNetTool {
    public static Integer detectFreePort(Integer[] ports) {
        Integer freePort = null;
        for (Integer port : ports) {
            try {
                ServerSocket serverSocket = new ServerSocket(port);
                serverSocket.close();
                freePort = port;
                break;
            } catch (IOException ex) {
                continue; // try next port
            }
        }
        return freePort;
    }
}
