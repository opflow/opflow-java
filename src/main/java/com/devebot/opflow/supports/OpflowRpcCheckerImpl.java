package com.devebot.opflow.supports;

/**
 *
 * @author acegik
 */
public class OpflowRpcCheckerImpl implements OpflowRpcChecker {

    @Override
    public Pong send(Ping info) {
        return new Pong();
    }
}
