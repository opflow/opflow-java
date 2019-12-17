package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public class OpflowRpcCheckerImpl implements OpflowRpcChecker {

    private static String sendSignature = "";

    @Override
    public Pong send(Ping info) {
        return new Pong();
    }
    
    public static String getSendSignature() throws NoSuchMethodException {
        if (sendSignature.length() == 0) {
            sendSignature = OpflowRpcChecker.class.getMethod("send", OpflowRpcChecker.Ping.class).toString();
        }
        return sendSignature;
    }
}
