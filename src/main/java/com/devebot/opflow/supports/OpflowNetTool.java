package com.devebot.opflow.supports;

import java.io.IOException;
import java.net.ServerSocket;

/**
 *
 * @author drupalex
 */
public class OpflowNetTool {
    public static Integer detectFreePort(Integer[] ports) {
        for (Integer port : ports) {
            if (isFreePort(port)) {
                return port;
            }
        }
        return null;
    }
    
    public static Integer detectFreePort(int min, int max) {
        Integer freePort = null;
        for (int port=min; port<max; port++) {
            if (isFreePort(port)) {
                freePort = port;
                break;
            }
        }
        return freePort;
    }
    
    public static boolean isFreePort(int port) {
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            serverSocket.close();
            return true;
        } catch (IOException ex) {
            return false;
        }
    }
}
