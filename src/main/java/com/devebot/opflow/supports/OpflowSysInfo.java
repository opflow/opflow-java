package com.devebot.opflow.supports;

/**
 *
 * @author pnhung177
 */
public class OpflowSysInfo {
    
    public static int getNumberOfProcessors() {
        return Runtime.getRuntime().availableProcessors();
    }
}
