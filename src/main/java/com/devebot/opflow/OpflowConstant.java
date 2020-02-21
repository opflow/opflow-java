package com.devebot.opflow;

/**
 *
 * @author acegik
 */
public class OpflowConstant {

    public final String COMPONENT_ID;
    public final String COMPONENT_TYPE;

    private OpflowConstant() {
        COMPONENT_ID = "componentId";
        COMPONENT_TYPE = "componentType";
    }
    
    private static final Object LOCK = new Object();
    private static OpflowConstant instance = null;

    public static OpflowConstant CURRENT() {
        if (instance == null) {
            synchronized (LOCK) {
                if (instance == null) {
                    instance = new OpflowConstant();
                }
            }
        }
        return instance;
    }
}
